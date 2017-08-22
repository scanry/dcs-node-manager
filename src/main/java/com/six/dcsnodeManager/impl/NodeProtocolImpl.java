package com.six.dcsnodeManager.impl;
/**   
* @author liusong  
* @date   2017年8月16日 
* @email  359852326@qq.com 
*/

import com.six.dcsnodeManager.Node;
import com.six.dcsnodeManager.api.NodeProtocol;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NodeProtocolImpl implements NodeProtocol{
	
	private AbstractDcsNodeManager dcsNodeManager;
	
	NodeProtocolImpl(AbstractDcsNodeManager dcsNodeManager){
		this.dcsNodeManager=dcsNodeManager;
	}

	@Override
	public Node getNewestNode() {
		return dcsNodeManager.getCurrentNode();
	}

	@Override
	public String reportToMaster(String slaveName) {
		if (dcsNodeManager.isMaster()) {
			log.info("receive report from " + slaveName);
			dcsNodeManager.getNodeRegister().listenNode(slaveName);
			return dcsNodeManager.getNodeRegister().getLocalUUID();
		}
		return null;
	}

}
