package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeContainerUpdateProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeContainerUpdateProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeContainerUpdate;


public class NodeContainerUpdatePBImpl extends  NodeContainerUpdate{

	 NodeContainerUpdateProto proto = NodeContainerUpdateProto.getDefaultInstance();
	 NodeContainerUpdateProto.Builder builder = null;
	 boolean viaProto = false;
	
	 private ContainerId containerId;
	 
	 
	 
	 public NodeContainerUpdatePBImpl() {
		    builder = NodeContainerUpdateProto.newBuilder();
	 }

	 public NodeContainerUpdatePBImpl(NodeContainerUpdateProto proto) {
		    this.proto = proto;
		    viaProto = true;
	 }
		  
	  public NodeContainerUpdateProto getProto() {
		    mergeLocalToProto();
		    proto = viaProto ? proto : builder.build();
		    viaProto = true;
		    return proto;
	  }
	  
	  private void mergeLocalToProto() {
		 if (viaProto){ 
		       maybeInitBuilder();
		  }
		  mergeLocalToBuilder();
		    proto = builder.build();
		    viaProto = true;
	  }

	  private void maybeInitBuilder() {
		if (viaProto || builder == null) {
		     builder = NodeContainerUpdateProto.newBuilder(proto);
		 }
		 viaProto = false;
	  }
	  
	  private void mergeLocalToBuilder() {
		 if(this.containerId != null) {
		      builder.setContainerId(
		      convertToProtoFormat(this.containerId));
		 }	 
	 }
	  
	@Override
	public void setContianerId(ContainerId containerId) {
	    maybeInitBuilder();
		if (containerId == null) 
		      builder.clearContainerId();
		this.containerId = containerId;
	}

	@Override
	public ContainerId getContainerId() {
		NodeContainerUpdateProtoOrBuilder p = viaProto ? proto : builder;
	    if (this.containerId != null) {
	      return this.containerId;
	    }
	    if (!p.hasContainerId()) {
	      return null;
	    }
	    this.containerId =  convertFromProtoFormat(p.getContainerId());
	    return this.containerId;
	}

	@Override
	public void setMemory(int memory) {
		 maybeInitBuilder();
		 builder.setMemory(memory);
	}

	@Override
	public int getMemory() {
		NodeContainerUpdateProtoOrBuilder p = viaProto ? proto : builder;
		return p.getMemory();
	}

	@Override
	public void setCores(int cores) {
		maybeInitBuilder();
		builder.setCpuCores(cores);
		
	}

	@Override
	public int getCores() {
		NodeContainerUpdateProtoOrBuilder p = viaProto ? proto : builder;
		return p.getCpuCores();
	}
	
    private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
		    return new ContainerIdPBImpl(p);
    }

    private ContainerIdProto convertToProtoFormat(ContainerId t) {
		    return ((ContainerIdPBImpl)t).getProto();
	}

	@Override
	public void setSuspend(boolean suspend) {
		maybeInitBuilder();
		builder.setSuspend(suspend);
	}

	@Override
	public boolean getSuspend() {
		NodeContainerUpdateProtoOrBuilder p = viaProto ? proto : builder;
		return p.getSuspend();
	}

	@Override
	public void setResume(boolean resume) {
		maybeInitBuilder();
		builder.setResume(resume);
	}

	@Override
	public boolean getResume() {
		NodeContainerUpdateProtoOrBuilder p = viaProto ? proto : builder;
		return p.getResume();
	}

}
