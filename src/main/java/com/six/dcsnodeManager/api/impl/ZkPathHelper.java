package com.six.dcsnodeManager.api.impl;

import java.util.Objects;

/**
 * @author liusong
 * @date 2017年8月10日
 * @email 359852326@qq.com
 * zookeeper 路径助手
 */
public class ZkPathHelper {

	/**存放集群主节点路径名称**/
	private static final String MASTER_NODE_PATH = "masternodes";
	/**存放集群从节点路径名称**/
	private static final String SLAVE_NODE_PATH = "slavenodes";
	/**存放集群缓存的路径名称**/
	private static final String CLUSTER_CACHE = "cache";
	/**存放集群锁的路径名称**/
	private static final String CLUSTER_LOCK = "lock";
	/**应用名称**/
	private final String appName;
	/**应用集群名称**/
	private final String clusterName;

	public ZkPathHelper(String appName, String clusterName) {
		Objects.requireNonNull(appName);
		Objects.requireNonNull(clusterName);
		this.appName = appName;
		this.clusterName = clusterName;
	}

	public String getRootPath() {
		return "/"+appName;
	}

	public String getClusterPath() {
		return getRootPath() + "/" + clusterName;
	}

	public String getMasterNodesPath() {
		return getClusterPath() + "/" + MASTER_NODE_PATH;
	}

	public String getMasterNodePath(String masterName) {
		return getMasterNodesPath() + "/" + masterName;
	}

	public String getSlaveNodesPath() {
		return getClusterPath() + "/" + SLAVE_NODE_PATH;
	}

	public String getSlaveNodePath(String slaveName) {
		return getSlaveNodesPath() + "/" + slaveName;
	}

	public String getSlaveNodeName(String workerNodePath) {
		String nodeName = workerNodePath.replace(getSlaveNodesPath() + "/", "");
		return nodeName;
	}

	public String getClusterCachesPath() {
		return getClusterPath() + "/" + CLUSTER_CACHE;
	}

	public String getClusterCachePath(String cacheName) {
		return getClusterCachesPath() + "/" + cacheName;
	}

	public String getClusterLocksPath() {
		return getClusterPath() + "/"+CLUSTER_LOCK;
	}

	public String getClusterLockPath(String lockPath) {
		return getClusterLocksPath() + "/" + lockPath;
	}

	public String getLatchPath(String latchPath) {
		return getClusterLocksPath() + "/" + latchPath;
	}

}
