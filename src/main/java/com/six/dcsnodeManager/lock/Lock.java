package com.six.dcsnodeManager.lock;
/**   
* @author liusong  
* @date   2017年8月1日 
* @email  359852326@qq.com 
*/
public interface Lock {

	void lockRead();
	
	void unlockRead();
	
	void lockWrite();
	
	void unlockWrite();
	
}
