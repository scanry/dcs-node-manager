package com.six.dcsnodeManager;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.six.dcsnodeManager.impl.IgniteDcsNodeManager;

/**   
* @author liusong  
* @date   2017年9月7日 
* @email  359852326@qq.com 
*/
public class ZkDcsNodeManager_3Test {

	final static Logger log = LoggerFactory.getLogger(ZkDcsNodeManager_1Test.class);

	public static void main(String[] args) throws Exception {

		String localHost = "127.0.0.1";
		int port = 8003;

		List<String> nodeStrs = new ArrayList<>();
		nodeStrs.add("127.0.0.1:8001");
		nodeStrs.add("127.0.0.1:8002");
		nodeStrs.add("127.0.0.1:8003");
		DcsNodeManager nodeManager = new IgniteDcsNodeManager(localHost, port, nodeStrs);
		nodeManager.registerNodeEvent(NodeEvent.MISS_SLAVE, missSlaveName -> {
			System.out.println("missed slave:" + missSlaveName);
		});
		nodeManager.registerNodeEvent(NodeEvent.MISS_MASTER, missSlaveName -> {
			System.out.println("missed master:" + missSlaveName);
		});
		nodeManager.registerNodeEvent(NodeEvent.BECOME_MASTER, master -> {
			System.out.println("选举成为主节点:" + master);
		});
		nodeManager.start();
		System.out.println("本地节点:" + nodeManager.getCurrentNode());
		System.out.println("是否为主节点:" + nodeManager.isMaster());
		Object wait = new ZkDcsNodeManager_2Test();
		synchronized (wait) {
			wait.wait();
		}
		nodeManager.shutdown();
	}
}
