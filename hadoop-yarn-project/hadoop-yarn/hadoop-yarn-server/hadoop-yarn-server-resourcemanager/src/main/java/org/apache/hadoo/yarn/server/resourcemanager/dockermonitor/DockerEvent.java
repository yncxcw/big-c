package org.apache.hadoo.yarn.server.resourcemanager.dockermonitor;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.AbstractEvent;

public class DockerEvent  extends 
AbstractEvent<DockerEventType>{
	
	private final ContainerId containerId;

	public DockerEvent(DockerEventType type, ContainerId containerId) {
		super(type);
		
		this.containerId = containerId;
		// TODO Auto-generated constructor stub
	}
	
	public ContainerId getContainerId() {
		return containerId;
	}
	
	

}
