package com.six.dcsnodeManager;

import com.six.dcsnodeManager.impl.ZkDcsNodeManager;

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
		ZkDcsNodeManager nodeManager=new ZkDcsNodeManager(appName, clusterName, masterNode, keepliveInterval, zkConnection,2,5);
		nodeManager.registerNodeEvent(NodeEvent.MISS_SLAVE,missSlaveName->{
			System.out.println("miss slave:"+missSlaveName);
		});
		nodeManager.registerNodeEvent(NodeEvent.MISS_MASTER,missSlaveName->{
			System.out.println("miss master:"+missSlaveName);
		});
		nodeManager.registerNodeEvent(NodeEvent.INIT_CLUSTER,master->{
			System.out.println("集群启动时第一个成为主节点事件:"+master);
		});
		nodeManager.registerNodeEvent(NodeEvent.BECOME_MASTER,master->{
			System.out.println("集群丢失master后重新选举成为主节点:"+master);
		});
		nodeManager.start();
		System.out.println("是否为主节点:"+nodeManager.isMaster());
		Object wait=new ZkDcsNodeManager_2Test();
		synchronized (wait) {
			wait.wait();
		}
		nodeManager.shutdown();
	}
}
