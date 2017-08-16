package com.six.dcsnodeManager.impl;

import java.util.Objects;

import org.apache.curator.framework.CuratorFramework;

import com.six.dcsnodeManager.api.ClusterCache;

/**   
* @author liusong  
* @date   2017年8月10日 
* @email  359852326@qq.com 
*/
public class ClusterCacheImpl implements ClusterCache{

	private CuratorFramework zkClient;
	private String path;
	
	public ClusterCacheImpl(CuratorFramework zkClient,String path){
		Objects.requireNonNull(zkClient);
		Objects.requireNonNull(path);
		this.zkClient=zkClient;
		this.path=path;
	}

	@Override
	public void set(String key, Object value) {
	}

	@Override
	public <T> T get(String key) {
		return null;
	}

	@Override
	public void clear() {
		
	}
}
