package com.six.dcsnodeManager.api.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.RetrySleeper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.recipes.leader.Participant;
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

	private CuratorFramework zkClient;

	private ZkNodeRegister zkNodeRegister;

	private ZkPathHelper zkPathHelper;

	private RpcServer rpcServer;

	private RpcClient rpcClient;

	private Set<NodeEventWatcher> missMasterEvents = new HashSet<>();
	private Set<NodeEventWatcher> missSlaveEvents = new HashSet<>();
	private Set<NodeEventWatcher> bbecomeMasterEvents = new HashSet<>();

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
		// String cachePath = zkPathHelper.getClusterCachePath(path);
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
	protected void updateSelfToRegister() {
		getNodeRegister().register(getCurrentNode());
	}

	@Override
	protected List<Node> getSlaveNodes() {
		return getNodeRegister().getSlaveNodes();
	}

	@Override
	public Lock newLock(String stamp) {
		String path = zkPathHelper.getClusterLockPath(stamp);
		final InterProcessReadWriteLock interProcessReadWriteLock = new InterProcessReadWriteLock(zkClient, path);
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

	class ZkNodeRegister implements NodeRegister {

		private final CountDownLatch leaderLatch = new CountDownLatch(1);
		private LeaderSelector leaderSelector;
		private String masterName;

		ZkNodeRegister() {
			register(getCurrentNode());
			leaderSelector = new LeaderSelector(zkClient, zkPathHelper.getClusterElectionPath(),
					new LeaderSelectorListenerAdapter() {
						@Override
						public void takeLeadership(final CuratorFramework client) throws Exception {
							leaderLatch.await();
						}
					});
			leaderSelector.autoRequeue();
			leaderSelector.setId(getCurrentNode().getName());
			leaderSelector.start();
		}

		@Override
		public synchronized void electionMaster() {
			while (true) {
				try {
					Participant masterParticipant = leaderSelector.getLeader();
					if (null != masterParticipant && masterParticipant.isLeader()
							&& StringUtils.isNotBlank(masterName = masterParticipant.getId())) {
						Node masterNode = getNode(masterName);
						if (!StringUtils.equals(masterName, getCurrentNode().getName())) {
							getCurrentNode().setStatus(NodeStatus.SLAVE);
							NodeProtocol npl = loolup(masterNode, NodeProtocol.class);
							npl.reportToMaster(getCurrentNode().getName());
							getNodeRegister().listenNode(masterNode.getName());
						} else {
							getCurrentNode().setStatus(NodeStatus.MASTER);
							updateSelfToRegister();
							for (NodeEventWatcher watcher : bbecomeMasterEvents) {
								watcher.process(masterName);
							}
						}
						break;
					}
					Thread.sleep(1000);
				} catch (Exception e) {
				}
			}
		}

		@Override
		public void register(Node node) {
			String nodePath = zkPathHelper.getNodePath(node.getName());
			register(nodePath, node);
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
			getCurrentNode().setStatus(NodeStatus.LOOKING);
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

		private void register(String path, Node node) {
			boolean isExist = false;
			try {
				if (null != zkClient.checkExists().forPath(path)) {
					isExist = true;
				}
			} catch (Exception e) {
				log.error("exist node[" + path + "] err", e);
			}
			byte[] data = SerializationUtils.serialize(node);
			if (isExist) {
				try {
					zkClient.setData().forPath(path, data);
				} catch (Exception e) {
					log.error("set node[" + path + "] data err", e);
				}
			} else {
				try {
					zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path, data);
				} catch (Exception e) {
					log.error("create node[" + path + "] err", e);
				}
			}
		}

		void close() {
			leaderLatch.countDown();
			try {
				leaderSelector.close();
			} catch (Exception e) {
				log.error("close leaderSelector err", e);
			}
		}
	}

	@Override
	protected void doShutdown() {
		if (null != rpcClient) {
			rpcClient.shutdown();
		}
		if (null != rpcServer) {
			rpcServer.shutdown();
		}
		if (null != zkClient) {
			zkClient.close();
		}
	}
}
