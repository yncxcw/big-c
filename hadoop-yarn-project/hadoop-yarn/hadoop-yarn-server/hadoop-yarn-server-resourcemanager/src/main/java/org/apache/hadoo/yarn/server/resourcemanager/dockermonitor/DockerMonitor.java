package org.apache.hadoo.yarn.server.resourcemanager.dockermonitor;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;



public interface DockerMonitor{
	
	public boolean Init(Configuration config);
	
	public boolean DehydrateContainer(ContainerId containerId);
	
	public boolean ResumeContainer(ContainerId containerId);
	
	public boolean UpdateContainerResource(ContainerId containerId, Resource resource);
	
	public List<DockerInfo> pollAppContainerStatus(ApplicationId applicationId);
	
	public List<DockerInfo> pollContainersStatus(List<ContainerId> containerIds);
	 	
}
