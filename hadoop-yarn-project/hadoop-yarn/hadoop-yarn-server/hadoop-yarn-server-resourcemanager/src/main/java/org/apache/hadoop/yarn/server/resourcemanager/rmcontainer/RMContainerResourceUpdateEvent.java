package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;

public class RMContainerResourceUpdateEvent extends RMContainerEvent {

	Resource resource;
	
	public RMContainerResourceUpdateEvent(ContainerId containerId,
			RMContainerEventType type, Resource resource) {
		super(containerId, type);
		
		this.resource = resource;
		// TODO Auto-generated constructor stub
	}
	
	public Resource getResource(){
		
		return resource;
	}

}
