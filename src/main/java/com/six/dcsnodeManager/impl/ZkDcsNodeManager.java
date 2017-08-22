package com.six.dcsnodeManager.impl;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.RetrySleeper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;

import com.six.dcsnodeManager.Lock;
import com.six.dcsnodeManager.Node;
import com.six.dcsnodeManager.api.ClusterCache;
import com.six.dcsnodeManager.api.NodeRegister;

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

	private CuratorFramework zkClient;

	private ZkNodeRegister zkNodeRegister;

	private ZkPathHelper zkPathHelper;

	private RpcServer rpcServer;

	private RpcClient rpcClient;

	public ZkDcsNodeManager(String appName, String clusterName, Node currentNode, long keepliveInterval,
			String zkConnection,int nodeRpcServerThreads,int nodeRpcClientThreads) {
		super(appName, clusterName,keepliveInterval);
		Objects.requireNonNull(currentNode);
		rpcServer = new NettyRpcServer(currentNode.getIp(), currentNode.getTrafficPort(),nodeRpcServerThreads);
		rpcClient = new NettyRpcCilent(nodeRpcClientThreads);
		this.zkPathHelper = new ZkPathHelper(appName, clusterName);
		this.zkClient = CuratorFrameworkUtils.newCuratorFramework(zkConnection, clusterName, new RetryPolicy() {
			@Override
			public boolean allowRetry(int retryCount, long elapsedTimeMs, RetrySleeper sleeper) {
				try {
					sleeper.sleepFor(elapsedTimeMs, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {}
				return true;
			}
		}, zkPathHelper);
		this.zkNodeRegister = new ZkNodeRegister(currentNode,this, zkClient, zkPathHelper);
	}

	@Override
	protected boolean isKeepalive(){
		return zkClient.getZookeeperClient().isConnected();
	}
	
	@Override
	protected NodeRegister getNodeRegister() {
		return zkNodeRegister;
	}

	@Override
	public ClusterCache newClusterCache(String path) {
		// String cachePath = zkPathHelper.getClusterCachePath(path);
		throw new UnsupportedOperationException();
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
