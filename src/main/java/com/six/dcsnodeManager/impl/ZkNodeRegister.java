package com.six.dcsnodeManager.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
import com.six.dcsnodeManager.NodeStatus;
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
	private String masterName;
	private Set<NodeEventWatcher> missMasterEvents = new HashSet<>();
	private Set<NodeEventWatcher> missSlaveEvents = new HashSet<>();
	private Set<NodeEventWatcher> bbecomeMasterEvents = new HashSet<>();

	ZkNodeRegister(ZkDcsNodeManager zkDcsNodeManager,CuratorFramework zkClient,ZkPathHelper zkPathHelper) {
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
		leaderSelector.setId(zkDcsNodeManager.getCurrentNode().getName());
		leaderSelector.start();
	}

	@Override
	public synchronized void electionMaster() {
		while (true) {
			try {
				register();
				Participant masterParticipant = leaderSelector.getLeader();
				if (null != masterParticipant && masterParticipant.isLeader()
						&& StringUtils.isNotBlank(masterName = masterParticipant.getId())) {
					Node masterNode = getNode(masterName);
					if (!StringUtils.equals(masterName, zkDcsNodeManager.getCurrentNode().getName())) {
						zkDcsNodeManager.getCurrentNode().setStatus(NodeStatus.SLAVE);
						NodeProtocol npl = zkDcsNodeManager.loolup(masterNode, NodeProtocol.class);
						npl.reportToMaster(zkDcsNodeManager.getCurrentNode().getName());
						listenNode(masterNode.getName());
					} else {
						zkDcsNodeManager.getCurrentNode().setStatus(NodeStatus.MASTER);
						for (NodeEventWatcher watcher : bbecomeMasterEvents) {
							watcher.process(masterName);
						}
					}
					break;
				}
				Thread.sleep(1000);
			} catch (Exception e) {
				log.error("node["+zkDcsNodeManager.getCurrentNode().getName()+"] launch an election err", e);
			}
		}
	}

	@Override
	public boolean register() {
		Node node=zkDcsNodeManager.getCurrentNode();
		String nodePath = zkPathHelper.getNodePath(node.getName());
		node.setLastKeepaliveTime(System.currentTimeMillis());
		boolean isExist = false;
		try {
			if (null != zkClient.checkExists().forPath(nodePath)) {
				isExist = true;
			}
		} catch (Exception e) {
			log.error("exist node[" + nodePath + "] err", e);
		}
		byte[] data = SerializationUtils.serialize(node);
		if (isExist) {
			try {
				zkClient.setData().forPath(nodePath, data);
				return true;
			} catch (Exception e) {
				log.error("set node[" + nodePath + "] data err", e);
			}
		} else {
			try {
				zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(nodePath, data);
				return true;
			} catch (Exception e) {
				log.error("create node[" + nodePath + "] err", e);
			}
		}
		return false;
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
		zkDcsNodeManager.getCurrentNode().setStatus(NodeStatus.LOOKING);
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
		if (NodeEvent.BECOME_MASTER == nodeEvent) {
			bbecomeMasterEvents.add(nodeEventWatcher);
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
