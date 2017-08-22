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

	String getLocalUUID();
	
	void electionMaster();
	
	void registerOrUpdate();
	
	Node getCurrentNode();
	
	Node getMaster();
	
	Node getNode(String nodeName);
	
	List<Node> getSlaveNodes();
	
	List<Node> getNodes();
	
	void listenNode(String nodeName);
	
	void registerNodeEvent(NodeEvent NodeEvent,NodeEventWatcher nodeEventWatcher);
	
	void close();
}
