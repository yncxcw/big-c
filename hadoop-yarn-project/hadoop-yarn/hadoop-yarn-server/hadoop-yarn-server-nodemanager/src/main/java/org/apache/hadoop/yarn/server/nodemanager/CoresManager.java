package org.apache.hadoop.yarn.server.nodemanager;

import java.util.Set;

import org.apache.hadoop.yarn.api.records.ContainerId;

public interface CoresManager {
	
	public void init();
	
	//called when allcoated container
	public Set<Integer> getAvailableCores(ContainerId cntId, int num);
	
	//called when a container finished execution
	public void releaseCores(ContainerId cntId);
	

}
