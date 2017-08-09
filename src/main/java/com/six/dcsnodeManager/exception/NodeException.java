package com.six.dcsnodeManager.exception;

/**
 * @author liusong
 * @date 2017年8月3日
 * @email 359852326@qq.com
 */
public class NodeException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -681149832070646247L;

	public NodeException(String message) {
		super(message);
	}

	public NodeException(String message, Throwable cause) {
		super(message, cause);
	}

}
