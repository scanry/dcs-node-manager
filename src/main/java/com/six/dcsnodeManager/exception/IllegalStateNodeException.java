package com.six.dcsnodeManager.exception;
/**   
* @author liusong  
* @date   2017年8月3日 
* @email  359852326@qq.com 
* 节点非法状态异常
*/
public class IllegalStateNodeException extends NodeException{

	/**
	 * 
	 */
	private static final long serialVersionUID = -7937323342333252424L;

	public IllegalStateNodeException(String message) {
		super(message);
	}

	public IllegalStateNodeException(String message, Throwable cause) {
		super(message, cause);
	}
}
