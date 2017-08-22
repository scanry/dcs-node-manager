package com.six.dcsnodeManager;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.Data;

@Data
public class Node implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -2889533327371829957L;

	/**节点名称**/
	private String name;
	
	/**
	 * 节点状态
	 * 0=looking
	 * 1=master
	 * 2=slave
	 **/
	private AtomicInteger status=new AtomicInteger(0);
	
	/**节点地址**/
	private String ip;
	
	/**节点端口**/
	private int port;
	
	/**节点间通信端口**/
	private int trafficPort;
	
	/**最后一次keepalive时间**/
	private long lastKeepaliveTime;
	
	private volatile float freeMemory;
	
	private volatile float cpu;
	
	
	public void looking(){
		status.set(0);
	}
	
	public void master(){
		status.set(1);
	}
	
	public void slave(){
		status.set(2);
	}
	
	public boolean isLooing(){
		return 0==status.get();
	}
	
	public boolean isMaster(){
		return 1==status.get();
	}
	
	public boolean isSlave(){
		return 2==status.get();
	}
	
}
