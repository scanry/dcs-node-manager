package com.six.dcsnodeManager;

import com.six.dcsnodeManager.api.impl.ZkDcsNodeManager;

/**   
* @author liusong  
* @date   2017年8月10日 
* @email  359852326@qq.com 
*/
public class ZkDcsNodeManager_2Test {


	public static void main(String[] args) throws InterruptedException {
		String appName="crawler";
		String clusterName="crawler_cluster";
		long keepliveInterval=2000;
		String zkConnection="127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";
		Node masterNode=new Node();
		masterNode.setName("test_2");
		masterNode.setIp("127.0.0.1");
		masterNode.setTrafficPort(8182);
		ZkDcsNodeManager masterNodeManager=new ZkDcsNodeManager(appName, clusterName, masterNode, keepliveInterval, zkConnection);
		masterNodeManager.registerNodeEvent(NodeEvent.MISS_SLAVE,missSlaveName->{
			System.out.println("miss slave:"+missSlaveName);
		});
		masterNodeManager.registerNodeEvent(NodeEvent.MISS_MASTER,missSlaveName->{
			System.out.println("miss master:"+missSlaveName);
		});
		masterNodeManager.registerNodeEvent(NodeEvent.BECOME_MASTER,missSlaveName->{
			System.out.println("成为主节点:"+missSlaveName);
		});
		masterNodeManager.start();
		System.out.println("是否为主节点:"+masterNodeManager.isMaster());
		Object wait=new ZkDcsNodeManager_2Test();
		synchronized (wait) {
			wait.wait(2000);
		}
		masterNodeManager.shutdown();
	}
}
