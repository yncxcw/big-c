package org.apache.hadoo.yarn.server.resourcemanager.dockermonitor;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;


public interface DockerMonitor  extends EventHandler<DockerEvent>{
	
	public void Init(RMContext rmContext);
	
	public boolean DehydrateContainer(ContainerId containerId);
	
	public boolean ResumeContainer(ContainerId containerId);
	
	public boolean UpdateContainerResource(ContainerId containerId, Resource resource);
	
	public List<DockerInfo> pollAppContainerStatus(ApplicationId applicationId);
	
	public List<DockerInfo> pollContainersStatus(List<ContainerId> containerIds);
	 	
}
