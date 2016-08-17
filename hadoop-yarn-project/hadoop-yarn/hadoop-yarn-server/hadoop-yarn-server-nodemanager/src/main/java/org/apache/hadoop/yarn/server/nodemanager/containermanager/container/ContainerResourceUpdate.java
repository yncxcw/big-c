package org.apache.hadoop.yarn.server.nodemanager.containermanager.container;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeContainerUpdate;

public class ContainerResourceUpdate extends ContainerEvent {
	
	NodeContainerUpdate nodeContainerUpdate;

	public ContainerResourceUpdate(ContainerId cID, NodeContainerUpdate nodeContainerUpdate) {
		super(cID, ContainerEventType.RESOURCE_UPDATE);
		this.nodeContainerUpdate = nodeContainerUpdate;
	}
	
	public NodeContainerUpdate getNodeContainerUpdate(){
		return this.nodeContainerUpdate;
	}

}
