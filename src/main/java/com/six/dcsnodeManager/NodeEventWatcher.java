package com.six.dcsnodeManager;

import java.util.UUID;

/**   
* @author liusong  
* @date   2017年8月1日 
* @email  359852326@qq.com 
*/
@FunctionalInterface
public interface NodeEventWatcher {

	void process(UUID uuid);
}
