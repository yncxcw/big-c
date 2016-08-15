package org.apache.hadoop.yarn.server.api.protocolrecords;

import java.util.Set;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.Records;

public abstract class NodeContainerUpdate {
	
	public static NodeContainerUpdate newInstance(ContainerId containerId, int memory,int cores,boolean suspend, boolean resume){
		NodeContainerUpdate nodeContainerUpdate =
		        Records.newRecord(NodeContainerUpdate.class);
		nodeContainerUpdate.setContianerId(containerId);
		nodeContainerUpdate.setMemory(memory);
		nodeContainerUpdate.setCores(cores);
		nodeContainerUpdate.setSuspend(suspend);
		nodeContainerUpdate.setResume(resume);
		return nodeContainerUpdate;
	}
	
	public abstract void setContianerId(ContainerId containerId);
	public abstract ContainerId getContainerId();
	
	public abstract void setMemory(int memory);
	public abstract int getMemory();
	
	public abstract void setCores(int cores);
	public abstract int getCores();
	
	public abstract void setSuspend(boolean suspend);
	public abstract boolean getSuspend();
	
	public abstract void setResume(boolean resume);
	public abstract boolean getResume();
	
}
