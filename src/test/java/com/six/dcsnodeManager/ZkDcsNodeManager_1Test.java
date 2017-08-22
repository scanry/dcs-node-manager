package com.six.dcsnodeManager;

import java.util.List;

import com.six.dcsnodeManager.api.DcsNodeManager;
import com.six.dcsnodeManager.impl.ZkDcsNodeManager;

/**
 * @author liusong
 * @date 2017年8月10日
 * @email 359852326@qq.com
 */
public class ZkDcsNodeManager_1Test {

	public static void main(String[] args) throws InterruptedException {
		
		
		Node localNode = new Node();
		localNode.setName("test_1");
		localNode.setIp("127.0.0.1");
		localNode.setTrafficPort(8181);
		DcsNodeManager nodeManager = new ZkDcsNodeManager("crawler", "crawler_cluster",
				localNode, 2000,"127.0.0.1:2181", 2, 5);
		nodeManager.registerNodeEvent(NodeEvent.MISS_SLAVE, missSlaveName -> {
			System.out.println("miss slave:" + missSlaveName);
		});
		nodeManager.registerNodeEvent(NodeEvent.MISS_MASTER, missMasterName -> {
			System.out.println("miss master:" + missMasterName);
		});
		nodeManager.registerNodeEvent(NodeEvent.INIT_CLUSTER, master -> {
			System.out.println("集群启动时第一个成为主节点事件:" + master);
		});
		nodeManager.registerNodeEvent(NodeEvent.BECOME_MASTER, master -> {
			System.out.println("集群丢失master后重新选举成为主节点:" + master);
		});
		nodeManager.start();
		System.out.println("是否为主节点:" + nodeManager.isMaster());
		List<NodeResource> resource=nodeManager.applyNodeResources(1, 5);
		nodeManager.returnNodeResources(resource);
		nodeManager.newLock("lock");//获取一把分布式锁
		nodeManager.registerService(ApplicationService.class, ApplicationService.instance);
		nodeManager.loolupService(localNode, ApplicationService.class);
		
		Object wait = new ZkDcsNodeManager_1Test();
		synchronized (wait) {
			wait.wait();
		}
	}
	
	private static Node getTargetNode(){
		return null;
	}
	
	static interface ApplicationService{
		static ApplicationService instance=new ApplicationService() {
		};
	}

}
