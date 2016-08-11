package org.apache.hadoop.yarn.server.api.protocolrecords;

import java.util.Set;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.Records;

public abstract class NodeContainerUpdate {
	
	public static NodeContainerUpdate newInstance(ContainerId containerId, int memory,Set<Integer> cpuCores){
		NodeContainerUpdate nodeContainerUpdate =
		        Records.newRecord(NodeContainerUpdate.class);
		nodeContainerUpdate.setContianerId(containerId);
		nodeContainerUpdate.setMemory(memory);
		nodeContainerUpdate.setcpuCores(cpuCores);
		return nodeContainerUpdate;
	}
	
	public abstract void setContianerId(ContainerId containerId);
	public abstract ContainerId getContainerId();
	
	public abstract void setMemory(int memory);
	public abstract int getMemory();
	
	public abstract void setcpuCores(Set<Integer> cpuCores);
	public abstract Set<Integer> getCpuCores();
	

}
