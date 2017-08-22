package com.six.dcsnodeManager.api;

import java.util.List;

import com.six.dcsnodeManager.Lock;
import com.six.dcsnodeManager.Node;
import com.six.dcsnodeManager.NodeEvent;
import com.six.dcsnodeManager.NodeResource;

import six.com.rpc.AsyCallback;

/**   
* @author liusong  
* @date   2017年7月31日 
* @email  359852326@qq.com 
* 节点服务接口
*/
public interface DcsNodeManager {

	String getAppName();
	/**
	 * 获取集群名称
	 * @return 当前集群名称，单机环境下返回默认名称
	 */
	String getClusterName();
	
	/**
	 * 返回当前节点
	 * @return 当前实例所在节点
	 */
	Node getCurrentNode();
	
	/**
	 * 返回当前集群主节点
	 * @return 当前集群主节点
	 */
	Node getMaster();
	
	List<Node> getSlaveNode();
	
	List<Node> getNodes();
	
	boolean isMaster();
	
	
	void start();
	
	/**
	 * 申请指定nodeResourcesNums 可用节点资源
	 * @param nodeNum 指点节点数
	 * @param threadNum 指点单节点线程数
	 * @return 小于等于nodeResourcesNums可用节点资源
	 */
	List<NodeResource> applyNodeResources(int nodeNum,int threadNum);
	
	/**
	 * 锁定指定节点资源
	 * @param list
	 */
	void lockNodeResources(List<NodeResource> list);
	
	/**
	 * 返回指定节点资源
	 * @param list
	 */
	void returnNodeResources(List<NodeResource> list);
	
	/**
	 * 注册节点事件处理
	 * @param NodeEvent 节点事件类型
	 * @param nodeEventWatcher  节点事件处理
	 */
	void registerNodeEvent(NodeEvent NodeEvent,NodeEventWatcher nodeEventWatcher);
	
	
	
	/**
	 * 通过path获取集群缓存
	 * @param path
	 * @return 返回可用的集群缓存实例
	 */
	ClusterCache newClusterCache(String path);
	
	/**
	 * 获取指定node上的指定异步服务(clz),支持成功调用回调
	 * @param node   指定节点
	 * @param clz    服务class
	 * @param asyCallback  回调操作
	 * @return  返回一个服务实例
	 */
	<T> T loolupService(Node node, Class<T> clz, AsyCallback asyCallback);

	/**
	 * 获取指定node上的指定同步服务(clz)
	 * @param node 指定节点
	 * @param clz  服务class
	 * @return 返回一个服务实例
	 */
	<T> T loolupService(Node node, Class<T> clz);
	
	void registerService(Class<?> protocol, Object instance);
	
	/**
	 * 通过stamp获取一个可用的可重入锁，单机或分布式锁由环境配置决定
	 * @param stamp 锁的标志
	 * @return 可重入锁
	 */
	Lock newLock(String stamp);
	
	void shutdown();
}
