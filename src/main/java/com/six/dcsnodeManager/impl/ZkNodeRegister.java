package com.six.dcsnodeManager.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

import com.six.dcsnodeManager.Node;
import com.six.dcsnodeManager.NodeEvent;
import com.six.dcsnodeManager.api.NodeEventWatcher;
import com.six.dcsnodeManager.api.NodeProtocol;
import com.six.dcsnodeManager.api.NodeRegister;

import lombok.extern.slf4j.Slf4j;

/**   
* @author liusong  
* @date   2017年8月16日 
* @email  359852326@qq.com 
*/
@Slf4j
public class ZkNodeRegister implements NodeRegister{

	private final CountDownLatch leaderLatch = new CountDownLatch(1);
	private ZkDcsNodeManager zkDcsNodeManager;
	private CuratorFramework zkClient;
	private ZkPathHelper zkPathHelper;
	private LeaderSelector leaderSelector;
	private Node currentNode;
	private String masterName;
	private Set<NodeEventWatcher> initCLuserEvents = new HashSet<>();
	private Set<NodeEventWatcher> missMasterEvents = new HashSet<>();
	private Set<NodeEventWatcher> missSlaveEvents = new HashSet<>();	                            
	private Set<NodeEventWatcher> becomeMasterEvents = new HashSet<>();
	private volatile String localUUID=UUID.randomUUID().toString();
	private static long electionInterval=1000;

	ZkNodeRegister(Node currentNode,ZkDcsNodeManager zkDcsNodeManager,CuratorFramework zkClient,ZkPathHelper zkPathHelper) {
		this.currentNode=currentNode;
		this.zkDcsNodeManager=zkDcsNodeManager;
		this.zkClient=zkClient;
		this.zkPathHelper=zkPathHelper;
		leaderSelector = new LeaderSelector(zkClient, zkPathHelper.getClusterElectionPath(),
				new LeaderSelectorListenerAdapter() {
					@Override
					public void takeLeadership(final CuratorFramework client) throws Exception {
						leaderLatch.await();
					}
				});
		leaderSelector.autoRequeue();
		leaderSelector.setId(currentNode.getName());
		leaderSelector.start();
	}

	@Override
	public String getLocalUUID(){
		return localUUID;
	}
	
	
	@Override
	public synchronized void electionMaster() {
		getCurrentNode().looking();
		registerOrUpdate();
		while (true) {
			try {
				Participant masterParticipant = leaderSelector.getLeader();
				if (null != masterParticipant && masterParticipant.isLeader()
						&& StringUtils.isNotBlank(masterName = masterParticipant.getId())) {
					if (!StringUtils.equals(masterName,currentNode.getName())) {
						currentNode.slave();
						Node masterNode = getNode(masterName);
						NodeProtocol npl = zkDcsNodeManager.loolupService(masterNode, NodeProtocol.class);
						localUUID=npl.reportToMaster(currentNode.getName());
						listenNode(masterNode.getName());
					}else{
						currentNode.master();
						String UUIDPath=zkPathHelper.getClusterStartUUIDPath();
						String existUUID=null;
						byte[] data=zkClient.getData().forPath(UUIDPath);
						if(null!=data){
							existUUID=SerializationUtils.deserialize(data);
						}
						//如果uuid相等，那么当前集群master是在第一个master挂掉后重新选举的。
						if(StringUtils.equals(localUUID, existUUID)){
							for (NodeEventWatcher watcher : becomeMasterEvents) {
								watcher.process(masterName);
							}
						}else{//如果不想等，说明整个集群重新启动
							zkClient.setData().forPath(UUIDPath,SerializationUtils.serialize(localUUID));
							for (NodeEventWatcher watcher : initCLuserEvents) {
								watcher.process(masterName);
							}
						}
					}
					registerOrUpdate();
					break;
				}
				Thread.sleep(electionInterval);
			} catch (Exception e) {
				log.error("node["+zkDcsNodeManager.getCurrentNode().getName()+"] launch an election err", e);
			}
		}
	}

	@Override
	public void registerOrUpdate() {
		String nodePath = zkPathHelper.getNodePath(currentNode.getName());
		currentNode.setLastKeepaliveTime(System.currentTimeMillis());
		boolean isExist = false;
		try {
			if (null != zkClient.checkExists().forPath(nodePath)) {
				isExist = true;
			}
		} catch (Exception e) {
			log.error("exist node[" + nodePath + "] err", e);
		}
		byte[] data = SerializationUtils.serialize(currentNode);
		if (isExist) {
			try {
				zkClient.setData().forPath(nodePath, data);
			} catch (Exception e) {
				log.error("set node[" + nodePath + "] data err", e);
			}
		} else {
			try {
				zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(nodePath, data);
			} catch (Exception e) {
				log.error("create node[" + nodePath + "] err", e);
			}
		}
	}

	@Override
	public Node getCurrentNode(){
		return currentNode;
	}
	
	@Override
	public Node getNode(String nodeName) {
		if (StringUtils.isNotBlank(nodeName)) {
			String nodePath = zkPathHelper.getNodePath(nodeName);
			try {
				byte[] data = zkClient.getData().forPath(nodePath);
				return SerializationUtils.deserialize(data);
			} catch (Exception e) {
				log.error("get node[" + nodeName + "] err", e);
			}
		}
		return null;
	}
	
	@Override
	public Node getMaster() {
		String masterPath = zkPathHelper.getNodePath(masterName);
		try {
			byte[] data = zkClient.getData().forPath(masterPath);
			return SerializationUtils.deserialize(data);
		} catch (Exception e) {
			log.error("get master[" + masterName + "] err", e);
		}
		return null;
	}

	@Override
	public List<Node> getSlaveNodes() {
		String nodesPath = zkPathHelper.getNodesPath();
		try {
			List<String> children = zkClient.getChildren().forPath(nodesPath);
			children.remove(masterName);
			String nodePath = null;
			for (String nodeName : children) {
				nodePath = zkPathHelper.getNodePath(nodeName);
				byte[] data = zkClient.getData().forPath(nodePath);
				return SerializationUtils.deserialize(data);
			}
		} catch (Exception e) {
			log.error("get slaveNodes err", e);
		}
		return null;
	}
	
	@Override
	public List<Node> getNodes(){
		String nodesPath = zkPathHelper.getNodesPath();
		try {
			List<String> children = zkClient.getChildren().forPath(nodesPath);
			String nodePath = null;
			for (String nodeName : children) {
				nodePath = zkPathHelper.getNodePath(nodeName);
				byte[] data = zkClient.getData().forPath(nodePath);
				return SerializationUtils.deserialize(data);
			}
		} catch (Exception e) {
			log.error("get slaveNodes err", e);
		}
		return null;
	}
	

	@Override
	public void listenNode(String nodeName) {
		String masterPath = zkPathHelper.getNodePath(nodeName);
		try {
			zkClient.checkExists().usingWatcher(new Watcher() {
				@Override
				public void process(WatchedEvent event) {
					String path = event.getPath();
					String missNodeName = zkPathHelper.getNodeName(path);
					if (EventType.NodeDeleted == event.getType()) {
						if (StringUtils.equals(masterName, missNodeName)) {
							doMissMaster(missNodeName);
						} else {
							doMissSlave(missNodeName);
						}
					} else {
						try {
							zkClient.checkExists().usingWatcher(this);
						} catch (Exception e) {
							log.error("watch path[" + path + "] failed", e);
						}
					}
				}
			}).forPath(masterPath);
		} catch (Exception e) {
			log.error("watch path[" + masterPath + "] failed", e);
		}
	}

	private void doMissMaster(String masterName) {
		for (NodeEventWatcher watcher : missMasterEvents) {
			watcher.process(masterName);
		}
		electionMaster();
	}

	private void doMissSlave(String slaveName) {
		for (NodeEventWatcher watcher : missSlaveEvents) {
			watcher.process(slaveName);
		}
	}


	@Override
	public void registerNodeEvent(NodeEvent nodeEvent, NodeEventWatcher nodeEventWatcher) {
		if (NodeEvent.INIT_CLUSTER == nodeEvent) {
			initCLuserEvents.add(nodeEventWatcher);
		} else if (NodeEvent.BECOME_MASTER == nodeEvent) {
			becomeMasterEvents.add(nodeEventWatcher);
		} else if (NodeEvent.MISS_MASTER == nodeEvent) {
			missMasterEvents.add(nodeEventWatcher);
		} else if (NodeEvent.MISS_SLAVE == nodeEvent) {
			missSlaveEvents.add(nodeEventWatcher);
		}
	}


	@Override
	public void close() {
		leaderLatch.countDown();
		try {
			leaderSelector.close();
		} catch (Exception e) {
			log.error("close leaderSelector err", e);
		}
	}

}
