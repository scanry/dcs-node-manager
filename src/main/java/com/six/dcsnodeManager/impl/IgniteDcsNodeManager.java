package com.six.dcsnodeManager.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.spi.collision.fifoqueue.FifoQueueCollisionSpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.six.dcsnodeManager.ClusterCache;
import com.six.dcsnodeManager.DcsNodeManager;
import com.six.dcsnodeManager.NodeEvent;
import com.six.dcsnodeManager.NodeEventWatcher;
import com.six.dcsnodeManager.lock.Lock;

/**
 * @author liusong
 * @date 2017年9月7日
 * @email 359852326@qq.com
 */
public class IgniteDcsNodeManager implements DcsNodeManager {

	final static Logger log = LoggerFactory.getLogger(IgniteDcsNodeManager.class);
	private static String DEFAULT__HOST = "127.0.0.1";
	private static int DEFAULT__PORT = 8881;
	private String host;
	private int port;
	private List<String> nodeStrs;
	private Ignite ignite;
	private volatile ClusterNode masterClusterNode;
	private ClusterNode localClusterNode;
	private Set<NodeEventWatcher> joinSlaveEvents = new LinkedHashSet<>();
	private Set<NodeEventWatcher> missMasterEvents = new LinkedHashSet<>();
	private Set<NodeEventWatcher> missSlaveEvents = new LinkedHashSet<>();
	private Set<NodeEventWatcher> becomeMasterEvents = new LinkedHashSet<>();

	public IgniteDcsNodeManager() {
		this(DEFAULT__HOST, DEFAULT__PORT, Collections.emptyList());
	}

	public IgniteDcsNodeManager(String host, int port, List<String> nodeStrs) {
		this.host = host;
		this.port = port;
		this.nodeStrs = nodeStrs;
	}

	@Override
	public synchronized void start() {
		if (null == ignite) {
			ignite = Ignition.getOrStart(getIgniteCfg());
			localClusterNode = ignite.cluster().localNode();
			electionMaster();
			listenNodeMiss();
			listenNodejoin();
		}
	}

	private IgniteConfiguration getIgniteCfg() {
		IgniteConfiguration cfg = new IgniteConfiguration();
		cfg.setPeerClassLoadingEnabled(true);
		/** Enable active on Start **/
		cfg.setActiveOnStart(true);
		/** Enable cache events for examples. **/
		cfg.setIncludeEventTypes(org.apache.ignite.events.EventType.EVTS_CACHE);
		/** TcpCommunicationSpi **/
		TcpCommunicationSpi tcpCommunicationSpi = new TcpCommunicationSpi();
		tcpCommunicationSpi.setSlowClientQueueLimit(1000);
		cfg.setCommunicationSpi(tcpCommunicationSpi);
		/** FifoQueueCollisionSpi **/
		FifoQueueCollisionSpi fifoQueueCollisionSpi = new FifoQueueCollisionSpi();
		fifoQueueCollisionSpi.setParallelJobsNumber(1);
		cfg.setCollisionSpi(fifoQueueCollisionSpi);
		/** 设置本地ip **/
		cfg.setLocalHost(host);
		/** 设置tpc节点发现 **/
		TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
		discoverySpi.setLocalAddress(host);
		discoverySpi.setLocalPort(port);
		TcpDiscoveryVmIpFinder tcpDiscoveryVmIpFinder = new TcpDiscoveryVmIpFinder();
		tcpDiscoveryVmIpFinder.setShared(true);
		if (null != nodeStrs) {
			tcpDiscoveryVmIpFinder.setAddresses(nodeStrs);
		}
		discoverySpi.setIpFinder(tcpDiscoveryVmIpFinder);
		cfg.setDiscoverySpi(discoverySpi);
		return cfg;
	}

	/**
	 * 监听丢失节点
	 */
	private void listenNodeMiss() {
		ignite.events().remoteListen((UUID, event) -> {
			UUID missUUID = getNodeUUIDFromEvent(event);
			log.info("missed node:" + missUUID);
			// 丢失主节点
			if (missUUID.equals(masterClusterNode.id())) {
				log.info("missed master:" + missUUID);
				for (NodeEventWatcher nodeEventWatcher : missMasterEvents) {
					nodeEventWatcher.process(missUUID);
				}
				electionMaster();
				if (isMaster()) {
					listenNodejoin();
				}
				for (NodeEventWatcher nodeEventWatcher : becomeMasterEvents) {
					nodeEventWatcher.process(localClusterNode.id());
				}
			} else if (isMaster()) {// 丢失从节点
				log.info("missed slave:" + missUUID);
				for (NodeEventWatcher nodeEventWatcher : missSlaveEvents) {
					nodeEventWatcher.process(missUUID);
				}
			}
			return true;
		}, null, EventType.EVT_NODE_FAILED);
	}

	/**
	 * 监听节点加入
	 */
	private void listenNodejoin() {
		ignite.events().remoteListen((uuid, event) -> {
			if (isMaster()) {
				UUID joinUuid = getNodeUUIDFromEvent(event);
				log.info("joined node:" + joinUuid);
				for (NodeEventWatcher nodeEventWatcher : joinSlaveEvents) {
					nodeEventWatcher.process(joinUuid);
				}
			}
			return true;
		}, null, EventType.EVT_NODE_JOINED);
	}

	private static UUID getNodeUUIDFromEvent(Event event) {
		String message = event.message();
		message = StringUtils.substringAfter(message, "[");
		String[] temp = StringUtils.split(message, ",");
		String id = StringUtils.remove(temp[0], "id=");
		return UUID.fromString(id);
	}

	@Override
	public ClusterNode getCurrentNode() {
		return localClusterNode;
	}

	@Override
	public ClusterNode getMaster() {
		return masterClusterNode;
	}

	private synchronized ClusterNode electionMaster() {
		return masterClusterNode = ignite.cluster().forOldest().node();
	}

	@Override
	public ClusterNode getNode(String nodeid) {
		return ignite.cluster().node(UUID.fromString(nodeid));
	}

	@Override
	public List<ClusterNode> getSlaveNodes() {
		return new ArrayList<>(ignite.cluster().forOthers(ignite.cluster().forOldest()).nodes());
	}

	@Override
	public List<ClusterNode> getNodes() {
		return new ArrayList<>(ignite.cluster().nodes());
	}

	@Override
	public boolean isMaster() {
		return getCurrentNode().id().equals(getMaster().id());
	}

	@Override
	public void registerNodeEvent(NodeEvent nodeEvent, NodeEventWatcher nodeEventWatcher) {
		if (NodeEvent.BECOME_MASTER == nodeEvent) {
			becomeMasterEvents.add(nodeEventWatcher);
		} else if (NodeEvent.MISS_SLAVE == nodeEvent) {
			missSlaveEvents.add(nodeEventWatcher);
		} else if (NodeEvent.MISS_MASTER == nodeEvent) {
			missMasterEvents.add(nodeEventWatcher);
		} else if (NodeEvent.JOIN_SLAVE == nodeEvent) {
			joinSlaveEvents.add(nodeEventWatcher);
		}

	}

	@Override
	public ClusterCache newClusterCache(String path) {
		return null;
	}

	@Override
	public <T> T loolupService(ClusterNode node, Class<T> clz, Object asyCallback) {
		return null;
	}

	@Override
	public <T> T loolupService(ClusterNode node, Class<T> clz) {
		return null;
	}

	@Override
	public void registerService(Class<?> protocol, Object instance) {

	}

	@Override
	public Lock newLock(String stamp) {
		return null;
	}

	@Override
	public void shutdown() {
		if (null != ignite) {
			ignite.close();
		}
	}

}
