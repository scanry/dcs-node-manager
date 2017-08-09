package com.six.dcsnodeManager.api.impl;

import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.six.dcsnodeManager.Lock;
import com.six.dcsnodeManager.Node;
import com.six.dcsnodeManager.NodeEvent;
import com.six.dcsnodeManager.NodeResource;
import com.six.dcsnodeManager.NodeStatus;
import com.six.dcsnodeManager.api.NodeEventWatcher;
import com.six.dcsnodeManager.api.NodeRegister;
import com.six.dcsnodeManager.api.NodeService;
import com.six.dcsnodeManager.api.NodeServiceProtocol;
import com.six.dcsnodeManager.exception.IllegalStateNodeException;

/**
 * @author liusong
 * @date 2017年8月1日
 * @email 359852326@qq.com
 */

public abstract class AbstractNodeService implements NodeService {

	final static Logger log = LoggerFactory.getLogger(AbstractNodeService.class);

	private String clusterName;

	private Node currentNode;
	
	private Node masterNode;
	
	private Lock electionlock;

	private Thread keepliveThread;

	private long keepliveInterval;
	
	private NodeRegister nodeRegister;
	
	private NodeServiceProtocol nodeServiceProtocol;

	public AbstractNodeService(String clusterName, Node currentNode, long keepliveInterval,NodeRegister nodeRegister) {
		Objects.requireNonNull(clusterName);
		Objects.requireNonNull(currentNode);
		this.clusterName = clusterName;
		this.currentNode = currentNode;
		this.keepliveInterval = keepliveInterval;
		this.nodeRegister=nodeRegister;
		electionlock=newLock("electionlock");
		electionMaster();
		keepliveThread = new Thread(() -> {
			keeplive();
		}, "node-keeplive-thread");
		keepliveThread.setDaemon(true);
		keepliveThread.start();
	}

	@Override
	public String getClusterName() {
		return clusterName;
	}

	@Override
	public Node getCurrentNode() {
		return currentNode;
	}

	private void keeplive() {
		while (true) {
			updateSelfToRegister();
			try {
				Thread.sleep(keepliveInterval);
			} catch (InterruptedException e) {
			}
		}
	}

	protected abstract Node doElectionMaster();
	
	protected abstract void updateSelfToRegister();

	protected abstract List<Node> getSlaveNodes();
	
	
	private Node electionMaster(){
		try{
			electionlock.lock();
			Node masterNode=electionMaster();
			if(masterNode.equals(currentNode)){
				currentNode.setStatus(NodeStatus.MASTER);
				nodeRegister.registerMaster(currentNode);
				nodeRegister.listenSlaves();
			}else{
				currentNode.setStatus(NodeStatus.SLAVE);
				nodeRegister.registerSlave(currentNode);
				nodeRegister.listenMaster();
			}		
			this.masterNode=masterNode;
		}catch (Exception e) {
			log.error("",e);
		}finally {
			electionlock.unlock();
		}
		return masterNode;
	}

	@Override
	public synchronized List<NodeResource> applyNodeResources(int nodeNum, int threadNum) {
		List<NodeResource> freeList=null;
		if(NodeStatus.MASTER==currentNode.getStatus()){
			
		}else if(NodeStatus.SLAVE==currentNode.getStatus()){
			
		}else{
			throw new IllegalStateNodeException("");
		}
		return freeList;
	}

	@Override
	public synchronized void lockNodeResources(List<NodeResource> list) {

	}

	@Override
	public synchronized void returnNodeResources(List<NodeResource> list) {
		// TODO Auto-generated method stub

	}

	@Override
	public void registerNodeEvent(NodeEvent NodeEvent, NodeEventWatcher nodeEventWatcher) {
		// TODO Auto-generated method stub

	}

	@Override
	public Lock newLock(String stamp) {
		// TODO Auto-generated method stub
		return null;
	}

}
