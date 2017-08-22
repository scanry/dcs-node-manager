package com.six.dcsnodeManager.api;

import com.six.dcsnodeManager.Node;

/**   
* @author liusong  
* @date   2017年8月3日 
* @email  359852326@qq.com 
*/
public interface NodeProtocol {

	/**
	 * 获取目标节点最新信息
	  * @return
	 */
	Node getNewestNode();
	
	/**
	 * 向主节点报道并返回主节点的启动uuid
	 * @param slaveName
	 * @return
	 */
	String reportToMaster(String slaveName);
}
