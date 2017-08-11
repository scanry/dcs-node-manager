package com.six.dcsnodeManager.api.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.RetrySleeper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.six.dcsnodeManager.Lock;
import com.six.dcsnodeManager.Node;
import com.six.dcsnodeManager.NodeEvent;
import com.six.dcsnodeManager.NodeStatus;
import com.six.dcsnodeManager.api.ClusterCache;
import com.six.dcsnodeManager.api.NodeEventWatcher;
import com.six.dcsnodeManager.api.NodeProtocol;
import com.six.dcsnodeManager.api.NodeRegister;

import six.com.rpc.AsyCallback;
import six.com.rpc.RpcClient;
import six.com.rpc.client.NettyRpcCilent;
import six.com.rpc.server.NettyRpcServer;
import six.com.rpc.server.RpcServer;

/**
 * @author liusong
 * @date 2017年8月9日
 * @email 359852326@qq.com
 */
public class ZkDcsNodeManager extends AbstractDcsNodeManager {

	final static Logger log = LoggerFactory.getLogger(ZkDcsNodeManager.class);

	private final static String ELECTION_STAMP="election";
	
	private CuratorFramework zkClient;

	private ZkNodeRegister zkNodeRegister;

	private ZkPathHelper zkPathHelper;

	private RpcServer rpcServer;

	private RpcClient rpcClient;
	
	private Set<NodeEventWatcher> missMasterEvents=new HashSet<>();
	private Set<NodeEventWatcher> missSlaveEvents=new HashSet<>();
	private Set<NodeEventWatcher> bbecomeMasterEvents=new HashSet<>();

	public ZkDcsNodeManager(String appName, String clusterName, Node currentNode, long keepliveInterval,
			String zkConnection) {
		super(appName, clusterName, currentNode, keepliveInterval);
		rpcServer = new NettyRpcServer(currentNode.getIp(), currentNode.getTrafficPort());
		rpcClient = new NettyRpcCilent();
		this.zkPathHelper = new ZkPathHelper(appName, clusterName);
		this.zkClient = CuratorFrameworkUtils.newCuratorFramework(zkConnection, clusterName, new RetryPolicy() {
			@Override
			public boolean allowRetry(int retryCount, long elapsedTimeMs, RetrySleeper sleeper) {
				return false;
			}
		}, zkPathHelper);
		this.zkNodeRegister = new ZkNodeRegister();
	}


	@Override
	protected NodeRegister getNodeRegister() {
		return zkNodeRegister;
	}

	@Override
	public ClusterCache newClusterCache(String path) {
		//String cachePath = zkPathHelper.getClusterCachePath(path);
		return null;
	}

	@Override
	public <T> T loolup(Node node, Class<T> clz, AsyCallback asyCallback) {
		return rpcClient.lookupService(node.getIp(), node.getTrafficPort(), clz, asyCallback);
	}

	@Override
	public <T> T loolup(Node node, Class<T> clz) {
		return rpcClient.lookupService(node.getIp(), node.getTrafficPort(), clz);
	}

	@Override
	protected Node electionMaster() {
		Node master=null;
		Lock electionlock=newLock(ELECTION_STAMP);
		try{
			electionlock.lock();
			Node existNode=getMaster();
			if(null==existNode||getCurrentNode().equals(existNode)){
				getCurrentNode().setStatus(NodeStatus.MASTER);
				updateSelfToRegister();
			}else{
				getCurrentNode().setStatus(NodeStatus.SLAVE);
				updateSelfToRegister();
				NodeProtocol npl=loolup(existNode, NodeProtocol.class);
				npl.reportToMaster(getCurrentNode().getName());
				getNodeRegister().listenMaster(existNode.getName());
			}		
		}catch (Exception e) {
			log.error("",e);
		}finally {
			electionlock.unlock();
		}
		return master;
	}

	@Override
	protected void updateSelfToRegister() {
		if (NodeStatus.MASTER == getCurrentNode().getStatus()) {
			getNodeRegister().registerMaster(getCurrentNode());
		} else {
			getNodeRegister().registerSlave(getCurrentNode());
		}
	}

	@Override
	protected List<Node> getSlaveNodes() {
		return getNodeRegister().getSlaveNodes();
	}

	@Override
	public Lock newLock(String stamp) {
		String path = zkPathHelper.getClusterLockPath(stamp);
		final InterProcessReadWriteLock interProcessReadWriteLock = new InterProcessReadWriteLock(zkClient,
				path);
		return new Lock() {
			@Override
			public void unlock() {
				try {
					interProcessReadWriteLock.writeLock().release();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
			@Override
			public void lock() {
				try {
					interProcessReadWriteLock.writeLock().acquire();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}

			}
		};
	}

	@Override
	protected RpcServer getRpcServer() {
		return rpcServer;
	}

	@Override
	protected RpcClient getRpcCilent() {
		return rpcClient;
	}
	
	class ZkNodeRegister implements NodeRegister{
		@Override
		public Node getMaster(){
			String mastersPath = zkPathHelper.getMasterNodesPath();
			try {
				List<String> masterPathList = zkClient.getChildren().forPath(mastersPath);
				if (null != masterPathList && masterPathList.size() == 1) {
					String masterPath = masterPathList.get(0);
					masterPath = zkPathHelper.getMasterNodePath(masterPath);
					byte[] data = zkClient.getData().forPath(masterPath);
					return SerializationUtils.deserialize(data);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			return null;
		}
		
		@Override
		public List<Node> getSlaveNodes(){
			return null;
		}
		
		@Override
		public void registerMaster(Node master) {
			String masterPath=zkPathHelper.getMasterNodePath(master.getName());
			register(masterPath, master);
		}

		@Override
		public void listenMaster(String masterName) {
			String masterPath=zkPathHelper.getMasterNodePath(masterName);
			try {
				zkClient.checkExists().usingWatcher(new Watcher() {
					@Override
					public void process(WatchedEvent event) {
						String path=event.getPath();
						if (EventType.NodeDeleted == event.getType()) {
							getCurrentNode().setStatus(NodeStatus.LOOKING);
							String masterName=path;
							for(NodeEventWatcher watcher:missMasterEvents){
								watcher.process(masterName);
							}
							Node master=electionMaster();
							if(isMaster()){
								for(NodeEventWatcher watcher:bbecomeMasterEvents){
									watcher.process(master.getName());
								}
							}
						}else {
							try {
								zkClient.checkExists().usingWatcher(this);
							} catch (Exception e) {
								log.error("watch path[" + path + "] failed",e);
							}
						}
					}}).forPath(masterPath);
			} catch (Exception e) {
				log.error("watch path[" + masterPath + "] failed",e);
			}
		}
		

		@Override
		public void registerSlave(Node slave) {
			String slavePath=zkPathHelper.getSlaveNodePath(slave.getName());
			register(slavePath, slave);
		}

		@Override
		public void listenSlave(String slaveName) {
			String slavePath=zkPathHelper.getSlaveNodePath(slaveName);
			try {
				zkClient.checkExists().usingWatcher(new Watcher() {
					@Override
					public void process(WatchedEvent event) {
						String path=event.getPath();
						if (EventType.NodeDeleted == event.getType()) {
							String slaveName=path;
							for(NodeEventWatcher watcher:missSlaveEvents){
								watcher.process(slaveName);
							}
							
						}else {
							try {
								zkClient.checkExists().usingWatcher(this);
							} catch (Exception e) {
								log.error("watch path[" + path + "] failed",e);
							}
						}
					}}).forPath(slavePath);
			} catch (Exception e) {
				log.error("watch path[" + slavePath + "] failed",e);
			}
		}
		
		@Override
		public void registerNodeEvent(NodeEvent nodeEvent,NodeEventWatcher nodeEventWatcher){
			if(NodeEvent.BECOME_MASTER==nodeEvent){
				bbecomeMasterEvents.add(nodeEventWatcher);
			}else if(NodeEvent.MISS_MASTER==nodeEvent){
				missMasterEvents.add(nodeEventWatcher);
			}else if(NodeEvent.MISS_SLAVE==nodeEvent){
				missSlaveEvents.add(nodeEventWatcher);
			}	
		}
		
		private void register(String path,Node node) {
			boolean isExist=false;
			try {
				if(null!=zkClient.checkExists().forPath(path)){
					isExist=true;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			byte[] data=SerializationUtils.serialize(node);
			if(isExist){
				try {
					zkClient.setData().forPath(path, data);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}else{
				try {
					zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path, data);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		
		}
	}

	@Override
	protected void doShutdown() {
		if(null!=rpcClient){
			rpcClient.shutdown();
		}
		if(null!=rpcServer){
			rpcServer.shutdown();
		}
		if(null!=zkClient){
			zkClient.close();
		}
	}
}
