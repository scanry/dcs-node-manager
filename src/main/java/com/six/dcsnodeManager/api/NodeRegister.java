package com.six.dcsnodeManager.api;

import com.six.dcsnodeManager.Node;

/**   
* @author liusong  
* @date   2017年8月3日 
* @email  359852326@qq.com 
*/
public interface NodeRegister {

	void registerMaster(Node master);
	
	void listenMaster();
	
	void registerSlave(Node slave);
	
	void listenSlaves();
}
