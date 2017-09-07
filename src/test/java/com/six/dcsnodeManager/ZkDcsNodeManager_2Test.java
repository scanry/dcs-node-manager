package com.six.dcsnodeManager;

import java.util.ArrayList;
import java.util.List;

import com.six.dcsnodeManager.impl.IgniteDcsNodeManager;

/**   
* @author liusong  
* @date   2017年8月10日 
* @email  359852326@qq.com 
*/
public class ZkDcsNodeManager_2Test {


	public static void main(String[] args) throws InterruptedException {
		String localHost = "127.0.0.1";
		int port = 8002;
		List<String> nodeStrs = new ArrayList<>();
		nodeStrs.add("127.0.0.1:8001");
		nodeStrs.add("127.0.0.1:8002");
		DcsNodeManager nodeManager = new IgniteDcsNodeManager(localHost, port, nodeStrs);nodeManager.registerNodeEvent(NodeEvent.MISS_SLAVE,missSlaveName->{
			System.out.println("missed slave:"+missSlaveName);
		});
		nodeManager.registerNodeEvent(NodeEvent.MISS_MASTER,missSlaveName->{
			System.out.println("missed master:"+missSlaveName);
		});
		nodeManager.registerNodeEvent(NodeEvent.JOIN_SLAVE,missSlaveName->{
			System.out.println("joined slave:"+missSlaveName);
		});
		nodeManager.registerNodeEvent(NodeEvent.BECOME_MASTER,master->{
			System.out.println("选举成为主节点:"+master);
		});
		nodeManager.start();
		System.out.println("本地节点:" + nodeManager.getCurrentNode());
		System.out.println("是否为主节点:"+nodeManager.isMaster());
		Object wait=new ZkDcsNodeManager_2Test();
		synchronized (wait) {
			wait.wait();
		}
		nodeManager.shutdown();
	}
}
