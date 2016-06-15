package org.apache.hadoo.yarn.server.resourcemanager.dockermonitor;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;

public abstract class  AbstractDockerMonitor implements DockerMonitor {
  
	boolean isWorking;
	
	public AbstractDockerMonitor(){
    //we should make sure if the Docker monitor daemon is working
		this.isWorking = false;
	}
	
	@Override
	public boolean Init(Configuration conf) {
	
		
		return false;
	}

	@Override
	public boolean DehydrateContainer(ContainerId containerId) {
		// we may do more check here, do it later
		String id = containerId.toString();
		DockerCommand command = new DockerCommand(id,null,DockerCommand.DEHYDRATE);
		return ExecuteCommand(command);
	}

	@Override
	public boolean ResumeContainer(ContainerId containerId) {
		// we may do more check here, do it later
		String id = containerId.toString();
		DockerCommand command = new DockerCommand(id,null,DockerCommand.RESUME);
		return ExecuteCommand(command);
	}

	@Override
	public boolean UpdateContainerResource(ContainerId containerId,Resource resource) {
		// we may do more check here, do it later
		String id = containerId.toString();
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
	
	public abstract void closeMonitor();
	
}
