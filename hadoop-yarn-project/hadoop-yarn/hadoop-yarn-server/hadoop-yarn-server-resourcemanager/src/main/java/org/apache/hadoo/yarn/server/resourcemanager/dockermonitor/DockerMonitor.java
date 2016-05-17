package org.apache.hadoo.yarn.server.resourcemanager.dockermonitor;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;


public interface DockerMonitor  extends EventHandler<DockerEvent>{
	
	public void DehydrateContainer(ContainerId containerId);
	
	public void ResumeContainer(ContainerId containerId);
	
	public void UpdateContainerResource(ContainerId containerId, Resource resource);
	
	public List<DockerInfo> pollAppContainerStatus(ApplicationId applicationId);
	
	public List<DockerInfo> pollContainersStatus(List<ContainerId> containerIds);
	 	
}
