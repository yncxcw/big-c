package org.apache.hadoop.yarn.server.nodemanager;

import java.util.List;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeContainerUpdate;

public class CMgrUpdateContainersEvent extends ContainerManagerEvent {
	
	List<NodeContainerUpdate> nodeContainerUpdates;

	public CMgrUpdateContainersEvent(List<NodeContainerUpdate> nodeContainerUpdates) {
		super(ContainerManagerEventType.UPDATE_CONTAINERS);
		this.nodeContainerUpdates = nodeContainerUpdates;
	}

	
	public List<NodeContainerUpdate> getNodeContainerUpdate(){
		
		return this.nodeContainerUpdates;
	}
	
}
