dcsnodeManager 分布式节点资源管理
=================================== 
### 一.	功能描述
1.	内部自动维护节点注册和选举，以及节点keeplive心跳。
2.	提供节点事件watcher注册。比如当前节点从集群孤立事件，丢失主节点，丢失工作节点，选举新的主节点等事件。
3.	提供节点间通信功能。
4.	提供集群节点资源申请和返回。
5.	提供集群缓存功能。
6.	提供集群读写锁。
---
### 二.	示例代码片段
		String appName="crawler";
		String clusterName="crawler_cluster";
		long keepliveInterval=2000;
		String zkConnection="127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";
		Node masterNode=new Node();
		masterNode.setName("test_1");
		masterNode.setIp("127.0.0.1");
		masterNode.setTrafficPort(8181);
		DcsNodeManager nodeManager=null;
		nodeManager=new ZkDcsNodeManager(appName,clusterName,masterNode,keepliveInterval,zkConnection); 
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
		int nodeNum=10;
		int threadNum=5;
		//申请10个节点，每个节点5个线程
		nodeManager.applyNodeResources(nodeNum, threadNum);
---