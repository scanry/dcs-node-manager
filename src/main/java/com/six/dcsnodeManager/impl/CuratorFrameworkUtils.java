package com.six.dcsnodeManager.impl;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**   
* @author liusong  
* @date   2017年8月10日 
* @email  359852326@qq.com 
*/
public class CuratorFrameworkUtils {


	final static Logger log = LoggerFactory.getLogger(CuratorFrameworkUtils.class);

	public static CuratorFramework newCuratorFramework(String connectStr, String clusterName, RetryPolicy retryPolicy,ZkPathHelper zkPathHelper) {
		CuratorFramework curatorFramework = null;
		curatorFramework = CuratorFrameworkFactory.newClient(connectStr, retryPolicy);
		curatorFramework.start();
		try {
			curatorFramework.blockUntilConnected();
		} catch (InterruptedException e) {
			log.error("connect zooKeeper[" + connectStr + "] err", e);
		}
		try {
			/**
			 * 初始化应用基本目录
			 */
			Stat stat = curatorFramework.checkExists().forPath(zkPathHelper.getRootPath());
			if (null == stat) {
				curatorFramework.create().withMode(CreateMode.PERSISTENT).forPath(zkPathHelper.getRootPath());
			}
			/**
			 * 初始化集群基本目录
			 */
			stat = curatorFramework.checkExists().forPath(zkPathHelper.getClusterPath());
			if (null == stat) {
				curatorFramework.create().withMode(CreateMode.PERSISTENT)
						.forPath(zkPathHelper.getClusterPath());
			}
			/**
			 * 初始化 node节点目录
			 */
			stat = curatorFramework.checkExists().forPath(zkPathHelper.getNodesPath());
			if (null == stat) {
				curatorFramework.create().withMode(CreateMode.PERSISTENT)
						.forPath(zkPathHelper.getNodesPath());
			}

			/**
			 * 初始化集群缓存目录
			 */
			stat = curatorFramework.checkExists().forPath(zkPathHelper.getClusterCachesPath());
			if (null == stat) {
				curatorFramework.create().withMode(CreateMode.PERSISTENT)
						.forPath(zkPathHelper.getClusterCachesPath());
			}
			
			/**
			 * 初始化集群锁目录
			 */
			stat = curatorFramework.checkExists().forPath(zkPathHelper.getClusterLocksPath());
			if (null == stat) {
				curatorFramework.create().withMode(CreateMode.PERSISTENT)
						.forPath(zkPathHelper.getClusterLocksPath());
			}
			
			/**
			 * 初始化集群启动uuid目录
			 */
			stat = curatorFramework.checkExists().forPath(zkPathHelper.getClusterStartUUIDPath());
			if (null == stat) {
				curatorFramework.create().withMode(CreateMode.PERSISTENT)
						.forPath(zkPathHelper.getClusterStartUUIDPath());
			}
		} catch (Exception e) {
			throw new RuntimeException("init zooKeeper's persistent path err",e);
		}
		return curatorFramework;
	}

}
