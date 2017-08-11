package com.six.dcsnodeManager.api;
/**   
* @author liusong  
* @date   2017年8月9日 
* @email  359852326@qq.com 
* 集群缓存
*/
public interface ClusterCache {

	void set(String key,Object value);
	
	<T>T get(String key);
	
	void clear();
}
