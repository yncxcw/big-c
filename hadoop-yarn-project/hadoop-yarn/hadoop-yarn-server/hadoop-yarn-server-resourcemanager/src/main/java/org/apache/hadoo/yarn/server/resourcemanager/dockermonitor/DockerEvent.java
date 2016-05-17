package org.apache.hadoo.yarn.server.resourcemanager.dockermonitor;

import org.apache.hadoop.yarn.event.AbstractEvent;

public class DockerEvent  extends 
AbstractEvent<DockerEventType>{

	public DockerEvent(DockerEventType type) {
		super(type);
		// TODO Auto-generated constructor stub
	}

}
