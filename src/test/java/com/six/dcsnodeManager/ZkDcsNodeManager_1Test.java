package com.six.dcsnodeManager;

import com.six.dcsnodeManager.api.DcsNodeManager;
import com.six.dcsnodeManager.api.impl.ZkDcsNodeManager;

/**   
* @author liusong  
* @date   2017年8月10日 
* @email  359852326@qq.com 
*/
public class ZkDcsNodeManager_1Test {

	public static void main(String[] args) throws InterruptedException {
		String appName="crawler";
		String clusterName="crawler_cluster";
		long keepliveInterval=2000;
		String zkConnection="127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";
		Node masterNode=new Node();
		masterNode.setName("test_1");
		masterNode.setIp("127.0.0.1");
		masterNode.setTrafficPort(8181);
		DcsNodeManager nodeManager=new ZkDcsNodeManager(appName, clusterName, masterNode, keepliveInterval, zkConnection);
		nodeManager.registerNodeEvent(NodeEvent.MISS_SLAVE,missSlaveName->{
			System.out.println("miss slave:"+missSlaveName);
		});
		nodeManager.registerNodeEvent(NodeEvent.MISS_MASTER,missMasterName->{
			System.out.println("miss master:"+missMasterName);
		});
		nodeManager.registerNodeEvent(NodeEvent.BECOME_MASTER,master->{
			System.out.println("成为主节点:"+master);
		});
		nodeManager.start();
		System.out.println("是否为主节点:"+nodeManager.isMaster());
		Object wait=new ZkDcsNodeManager_1Test();
		synchronized (wait) {
			wait.wait();
		}
	}

}
