package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;

import org.apache.hadoop.yarn.api.records.ContainerId;

public class RMContainerResumeEvent extends RMContainerEvent {

	public RMContainerResumeEvent(ContainerId containerId,
			RMContainerEventType type) {
		super(containerId, type);
		// TODO Auto-generated constructor stub
	}

}
