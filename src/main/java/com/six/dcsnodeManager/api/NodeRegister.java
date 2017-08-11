package com.six.dcsnodeManager.api;

import java.util.List;

import com.six.dcsnodeManager.Node;
import com.six.dcsnodeManager.NodeEvent;

/**   
* @author liusong  
* @date   2017年8月3日 
* @email  359852326@qq.com 
*/
public interface NodeRegister {

	Node getMaster();
	
	List<Node> getSlaveNodes();
	
	void registerMaster(Node master);
	
	void listenMaster(String masterName);
	
	void registerSlave(Node slave);
	
	void listenSlave(String slaveName);
	
	void registerNodeEvent(NodeEvent NodeEvent,NodeEventWatcher nodeEventWatcher);
}
