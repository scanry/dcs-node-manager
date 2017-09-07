package com.six.dcsnodeManager;

/**
 * @author liusong
 * @date 2017年8月1日
 * @email 359852326@qq.com
 */
public enum NodeEvent {
	/** 加入新的从节点 **/
	JOIN_SLAVE,
	/** 丢失主节点事件 **/
	MISS_MASTER,
	/** 丢失从节点事件 **/
	MISS_SLAVE,
	/** 成为主节点事件 **/
	BECOME_MASTER
}
