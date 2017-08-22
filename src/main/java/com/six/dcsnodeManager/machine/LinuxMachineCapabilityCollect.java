package com.six.dcsnodeManager.machine;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.commons.lang3.StringUtils;

/**
 * @author liusong
 * @date 2017年8月16日
 * @email 359852326@qq.com
 */
public class LinuxMachineCapabilityCollect extends MachineCapabilityCollect {

	protected MachineUseageRateInfo getMachineUseageRateInfo() {
		InputStream is = null;
		InputStreamReader isr = null;
		BufferedReader brStat = null;
		BigDecimal cpuUsageRate = new BigDecimal(0);
		BigDecimal netWorkUsageRate = new BigDecimal(0);
		BigDecimal recordCountBg = new BigDecimal(2);
		int recordCount = 0;
		do {
			try {
				Process process = Runtime.getRuntime().exec("top");
				is = process.getInputStream();
				isr = new InputStreamReader(is);
				brStat = new BufferedReader(isr);
				String line = null;
				int readCount = 0;
				while (null != (line = brStat.readLine())) {
					if (StringUtils.containsIgnoreCase(line, "CPU usage")) {
						cpuUsageRate = cpuUsageRate.add(doCpuUsageRate(line));
						readCount++;
					} else if (StringUtils.containsIgnoreCase(line, "Networks")) {
						netWorkUsageRate = netWorkUsageRate.add(doNetworksUsageRate(line));
						readCount++;
					}
					if (2 == readCount) {
						break;
					}
				}
			} catch (IOException ioe) {
			} finally {
				close(brStat);
				close(isr);
				close(is);
			}
			try {
				Thread.sleep(RECORD_WAIT_TIME);
			} catch (InterruptedException e) {
			}
		} while (++recordCount < 2);
		int totalMemory = (int) Runtime.getRuntime().totalMemory();
		int freeMemory = (int) Runtime.getRuntime().freeMemory();
		BigDecimal totalMemoryBg = new BigDecimal(totalMemory);
		BigDecimal freeMemoryBg = new BigDecimal(freeMemory);
		MachineUseageRateInfo machineUseageRateInfo = new MachineUseageRateInfo();
		machineUseageRateInfo.setCpuFreeRate(cpuUsageRate.divide(recordCountBg, 4, RoundingMode.DOWN).floatValue());
		machineUseageRateInfo.setMemoryfreeRate(freeMemoryBg.divide(totalMemoryBg, 4, RoundingMode.DOWN).floatValue());
		machineUseageRateInfo
				.setNetworkFreeRate(netWorkUsageRate.divide(recordCountBg, 4, RoundingMode.DOWN).floatValue());
		return machineUseageRateInfo;
	}

	private BigDecimal doCpuUsageRate(String infoStr) {
		infoStr = StringUtils.remove(infoStr, "CPU usage:");
		String[] infos = StringUtils.split(infoStr, ",");
		String idleCpu = infos[2];
		idleCpu = StringUtils.remove(idleCpu, "idle");
		idleCpu = StringUtils.remove(idleCpu, "%");
		idleCpu = StringUtils.trim(idleCpu);
		return new BigDecimal(idleCpu);
	}

	private BigDecimal doNetworksUsageRate(String infoStr) {
		infoStr = StringUtils.remove(infoStr, "Networks: packets:");
		String[] infos = StringUtils.split(infoStr, ",");
		String inNetwork = infos[0];
		inNetwork = StringUtils.remove(inNetwork, "M");
		inNetwork = StringUtils.remove(inNetwork, "in");
		inNetwork = StringUtils.trim(inNetwork);
		String[] temp = StringUtils.split(inNetwork, "/");
		int usageNetWorks = Integer.valueOf(temp[0]);
		int allNetWorks = Integer.valueOf(temp[1]) * 1024 * 1024;
		BigDecimal usageNetWorksBd = new BigDecimal(usageNetWorks);
		BigDecimal allNetWorksBd = new BigDecimal(allNetWorks);
		BigDecimal networksUsageRate = usageNetWorksBd.divide(allNetWorksBd, 4, RoundingMode.HALF_EVEN);
		return networksUsageRate;
	}
}
