package org.apache.hadoo.yarn.server.resourcemanager.dockermonitor;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.Resource;

public class DockerCommand {
	
	static int DEHYDRATE = 00; 
	static int RESUME    = 01;
	static int UPDATE    = 02;
	
	String containerId;
	
	//updated resource
	Resource resource;

	//command type, now we support 3 commands
	int type;
	
	public DockerCommand(String id, Resource resource, int type){
		this.containerId = id;
		this.resource    = resource;
		this.type        = type;
	}
	
	public String getContainerId() {
		return containerId;
	}

	public Resource getResource() {
		return resource;
	}

	public int getType() {
		return type;
	}
	
	static Map<String,String> commandToMap(DockerCommand command){
		Map<String,String> dockerCommand = new HashMap<String,String>();
		dockerCommand.put("containerId", command.getContainerId());
		dockerCommand.put("commandType", Integer.toString(command.getType()));
		dockerCommand.put("resource.memory", Integer.toString(command.getResource().getMemory()));
		dockerCommand.put("resource.cores", Integer.toString(command.getResource().getVirtualCores()));
		return dockerCommand;
	}
}
