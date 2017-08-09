package com.six.dcsnodeManager;

import lombok.Data;

@Data
public class Node {

	/**节点名称**/
	private String name;
	
	/**节点状态**/
	private NodeStatus status=NodeStatus.LOOKING;
	
	/**节点地址**/
	private String ip;
	
	/**节点端口**/
	private int port;
	
	/**节点间通信端口**/
	private int trafficPort;
}
