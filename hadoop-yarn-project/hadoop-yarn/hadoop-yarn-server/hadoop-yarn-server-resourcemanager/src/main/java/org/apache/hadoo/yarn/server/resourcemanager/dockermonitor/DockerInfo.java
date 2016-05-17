package org.apache.hadoo.yarn.server.resourcemanager.dockermonitor;

import java.util.Map;

import org.apache.hadoop.yarn.api.records.ContainerId;

public class DockerInfo {
	
	ContainerId containerId;
	
	Map<String,String> cpuSetInfo;
	
    Map<String,String> cpuInfo;
	
	Map<String,String> diskInfo;
	
	Map<String,String> networkInfo;
	
	public DockerInfo(ContainerId containerId, Map<String, String> cpuSetInfo,
			Map<String, String> cpuInfo, Map<String, String> diskInfo,
			Map<String, String> networkInfo) {
		super();
		this.containerId = containerId;
		this.cpuSetInfo = cpuSetInfo;
		this.cpuInfo = cpuInfo;
		this.diskInfo = diskInfo;
		this.networkInfo = networkInfo;
	}

	public ContainerId getContainerId() {
		return containerId;
	}

	public void setContainerId(ContainerId containerId) {
		this.containerId = containerId;
	}

	public Map<String, String> getCpuSetInfo() {
		return cpuSetInfo;
	}

	public void setCpuSetInfo(Map<String, String> cpuSetInfo) {
		this.cpuSetInfo = cpuSetInfo;
	}

	public Map<String, String> getCpuInfo() {
		return cpuInfo;
	}

	public void setCpuInfo(Map<String, String> cpuInfo) {
		this.cpuInfo = cpuInfo;
	}

	public Map<String, String> getDiskInfo() {
		return diskInfo;
	}

	public void setDiskInfo(Map<String, String> diskInfo) {
		this.diskInfo = diskInfo;
	}

	public Map<String, String> getNetworkInfo() {
		return networkInfo;
	}

	public void setNetworkInfo(Map<String, String> networkInfo) {
		this.networkInfo = networkInfo;
	}

}
