package com.six.dcsnodeManager.api.impl;

import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.six.dcsnodeManager.Node;
import com.six.dcsnodeManager.NodeEvent;
import com.six.dcsnodeManager.NodeResource;
import com.six.dcsnodeManager.NodeStatus;
import com.six.dcsnodeManager.api.NodeEventWatcher;
import com.six.dcsnodeManager.api.NodeProtocol;
import com.six.dcsnodeManager.api.NodeRegister;
import com.six.dcsnodeManager.api.DcsNodeManager;
import com.six.dcsnodeManager.exception.IllegalStateNodeException;

import six.com.rpc.RpcClient;
import six.com.rpc.server.RpcServer;

/**
 * @author liusong
 * @date 2017年8月1日
 * @email 359852326@qq.com
 */

public abstract class AbstractDcsNodeManager implements DcsNodeManager {

	final static Logger log = LoggerFactory.getLogger(AbstractDcsNodeManager.class);

	private String appName;

	private String clusterName;

	private Node currentNode;

	private Thread keepliveThread;

	private long keepliveInterval;
	
	private volatile boolean shutdown;

	public AbstractDcsNodeManager(String appName, String clusterName, Node currentNode, long keepliveInterval) {
		Objects.requireNonNull(appName);
		Objects.requireNonNull(clusterName);
		Objects.requireNonNull(currentNode);
		this.appName = appName;
		this.clusterName = clusterName;
		this.currentNode = currentNode;
		this.keepliveInterval = keepliveInterval;
		keepliveThread = new Thread(() -> {
			keeplive();
		}, "node-keeplive-thread");
		keepliveThread.setDaemon(true);
	}

	@Override
	public String getAppName() {
		return appName;
	}

	@Override
	public String getClusterName() {
		return clusterName;
	}

	@Override
	public Node getCurrentNode() {
		return currentNode;
	}

	@Override
	public synchronized boolean isMaster() {
		return NodeStatus.MASTER == getCurrentNode().getStatus();
	}

	@Override
	public void start() {
		NodeProtocol selfNodeProtocol = new NodeProtocol() {
			@Override
			public void reportToMaster(String slaveName) {
				if (isMaster()) {
					log.info("receive report from " + slaveName);
					getNodeRegister().listenSlave(slaveName);
				}
			}

			@Override
			public Node getNewestNode() {
				return getCurrentNode();
			}
		};
		getRpcServer().register(NodeProtocol.class, selfNodeProtocol);
		electionMaster();
		keepliveThread.start();
	}

	private void keeplive() {
		while (shutdown) {
			updateSelfToRegister();
			try {
				Thread.sleep(keepliveInterval);
			} catch (InterruptedException e) {
			}
		}
	}

	@Override
	public Node getMaster() {
		return getNodeRegister().getMaster();
	}

	@Override
	public synchronized List<NodeResource> applyNodeResources(int nodeNum, int threadNum) {
		List<NodeResource> freeList = null;
		if (NodeStatus.MASTER == currentNode.getStatus()) {

		} else if (NodeStatus.SLAVE == currentNode.getStatus()) {

		} else {
			throw new IllegalStateNodeException("");
		}
		return freeList;
	}

	@Override
	public synchronized void lockNodeResources(List<NodeResource> list) {

	}

	@Override
	public synchronized void returnNodeResources(List<NodeResource> list) {

	}

	@Override
	public void registerNodeEvent(NodeEvent NodeEvent, NodeEventWatcher nodeEventWatcher) {
		getNodeRegister().registerNodeEvent(NodeEvent, nodeEventWatcher);
	}
	
	public final synchronized void shutdown(){
		shutdown=true;
		doShutdown();
	}

	protected abstract NodeRegister getNodeRegister();

	protected abstract Node electionMaster();

	protected abstract void updateSelfToRegister();

	protected abstract List<Node> getSlaveNodes();

	protected abstract RpcServer getRpcServer();

	protected abstract RpcClient getRpcCilent();
	
	protected abstract void doShutdown();
	
	
}
