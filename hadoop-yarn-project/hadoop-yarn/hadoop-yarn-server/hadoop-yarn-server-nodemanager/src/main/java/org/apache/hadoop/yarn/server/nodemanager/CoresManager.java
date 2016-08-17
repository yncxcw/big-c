package org.apache.hadoop.yarn.server.nodemanager;

import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;

public interface CoresManager {
	
	public void init(Configuration conf);
	
	//called when allcoated container
	public Set<Integer> allocateCores(ContainerId cntId, int num);
	
	//preempt #num of cores and returned new assigned cores
	public Set<Integer> resetCores(ContainerId cntId, int num);
	
	//called when a container finished execution
	public void releaseCores(ContainerId cntId);
	

}
