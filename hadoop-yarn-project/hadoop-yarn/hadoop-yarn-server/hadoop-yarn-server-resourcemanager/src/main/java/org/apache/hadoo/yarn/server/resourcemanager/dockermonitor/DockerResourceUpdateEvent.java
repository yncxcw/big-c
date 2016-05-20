package org.apache.hadoo.yarn.server.resourcemanager.dockermonitor;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;

public class DockerResourceUpdateEvent extends DockerEvent {

	Resource resource;
	
	public DockerResourceUpdateEvent(DockerEventType type,
			ContainerId containerId, Resource resource) {
		super(type, containerId);
		this.resource = resource;
		// TODO Auto-generated constructor stub
	}
	
	public Resource getResource() {
		return resource;
	}

}
