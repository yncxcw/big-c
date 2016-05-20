package org.apache.hadoo.yarn.server.resourcemanager.dockermonitor;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;

public abstract class  AbstractDockerMonitor implements DockerMonitor {
    
	RMContext rmContext;
	
	boolean isWorking;
	
	@Override
	public void Init(RMContext rmContext) {
		this.rmContext = rmContext;
		// TODO Auto-generated method stub
	}

	@Override
	public boolean DehydrateContainer(ContainerId containerId) {
		// we may do more check here, do it later
		String id = Long.toString(containerId.getContainerId());
		DockerCommand command = new DockerCommand(id,null,DockerCommand.DEHYDRATE);
		return ExecuteCommand(command);
	}

	@Override
	public boolean ResumeContainer(ContainerId containerId) {
		// we may do more check here, do it later
		String id = Long.toString(containerId.getContainerId());
		DockerCommand command = new DockerCommand(id,null,DockerCommand.RESUME);
		return ExecuteCommand(command);
	}

	@Override
	public boolean UpdateContainerResource(ContainerId containerId,Resource resource) {
		// we may do more check here, do it later
		String id = Long.toString(containerId.getContainerId());
		DockerCommand command = new DockerCommand(id,resource,DockerCommand.UPDATE);
		return ExecuteCommand(command);
	}

	@Override
	public List<DockerInfo> pollAppContainerStatus(ApplicationId applicationId) {
		// TODO Auto-generated method stub,leave it for future use
		return null;
	}

	@Override
	public List<DockerInfo> pollContainersStatus(List<ContainerId> containerIds) {
		// TODO Auto-generated method stub,leave it for future use
		return null;
	}
	
	public abstract boolean ExecuteCommand(DockerCommand command);
	
	@Override
	public void handle(DockerEvent event) {
		// TODO Auto-generated method stub
		ContainerId containerId = event.getContainerId();
		switch(event.getType()){
		   case SUSPEND_CONTAIENR:DehydrateContainer(containerId);
			   break;
		   case RESUME_CONTAINER:ResumeContainer(containerId);
		       break;
		   case UPDATE_CONTAIENR:DockerResourceUpdateEvent revent = (DockerResourceUpdateEvent) event;
		       Resource resource = revent.getResource();
			   UpdateContainerResource(containerId,resource);
		       break;
		   case MONITOR_CONTAIENR:
			   //TODO we are not supporting yet
			   break;
		}
		
	}

}
