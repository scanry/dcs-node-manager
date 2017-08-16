package com.six.dcsnodeManager.impl;

import java.util.Objects;

import org.apache.commons.lang3.StringUtils;

/**
 * @author liusong
 * @date 2017年8月10日
 * @email 359852326@qq.com
 * zookeeper 路径助手
 */
public class ZkPathHelper {

	/**存放集群节点路径名称**/
	private static final String NODE_PATH = "nodes";
	/**存放集群缓存的路径名称**/
	private static final String CLUSTER_CACHE = "cache";
	/**存放集群锁的路径名称**/
	private static final String CLUSTER_LOCK = "lock";
	/**存放集群选举路径名称**/
	private static final String ELECTION = "election";
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


	public String getNodesPath() {
		return getClusterPath() + "/" + NODE_PATH;
	}

	public String getNodePath(String nodeName) {
		return getNodesPath() + "/" + nodeName;
	}
	
	public String getNodeName(String nodePath) {
		return StringUtils.remove(nodePath, getNodesPath() + "/");
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

	public String getClusterElectionPath() {
		return getClusterPath() + "/" + ELECTION;
	}

}
