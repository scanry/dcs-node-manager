package com.six.dcsnodeManager.machine;

import java.io.Closeable;
import java.io.IOException;


import org.apache.commons.lang3.StringUtils;

/**
 * @author liusong
 * @date 2017年8月16日
 * @email 359852326@qq.com
 * 机器性能收集
 */
public abstract class MachineCapabilityCollect {

	static final int RECORD_WAIT_TIME = 500;
	static MachineCapabilityCollect INSTANCE;
	private static String osName;
	static {
		osName = System.getProperty("os.name");
		if(StringUtils.containsIgnoreCase(osName, "win")){
			INSTANCE=null;
		}else if(StringUtils.containsIgnoreCase(osName, "Mac")){
			INSTANCE=new LinuxMachineCapabilityCollect();
		}else{
			INSTANCE=new LinuxMachineCapabilityCollect();
		}
	}
	protected abstract MachineUseageRateInfo getMachineUseageRateInfo();
	static void close(Closeable closeable) {
		if (null != closeable) {
			try {
				closeable.close();
			} catch (IOException e) {
			}
		}
	}
	public static void main(String[] args) {
		MachineUseageRateInfo machineUseageRateInfo=MachineCapabilityCollect.INSTANCE.getMachineUseageRateInfo();
		System.out.println("空闲内存率:" + machineUseageRateInfo.getMemoryfreeRate());
		System.out.println("cpu空闲率:"+machineUseageRateInfo.getCpuFreeRate());
		System.out.println("网络使用率:"+machineUseageRateInfo.getNetworkFreeRate());
	}

}
