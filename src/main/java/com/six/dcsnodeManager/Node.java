package com.six.dcsnodeManager;

import java.io.Serializable;

import lombok.Data;

@Data
public class Node implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -2889533327371829957L;

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
	
	/**最后一次keepalive时间**/
	private long lastKeepaliveTime;
}
