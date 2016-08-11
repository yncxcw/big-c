package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerStatusProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.NodeStatusProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeContainerUpdateProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeContainerUpdateProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeHeartbeatRequestProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeContainerUpdate;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.api.records.impl.pb.NodeStatusPBImpl;

public class NodeContainerUpdatePBImpl extends  NodeContainerUpdate{

	 NodeContainerUpdateProto proto = NodeContainerUpdateProto.getDefaultInstance();
	 NodeContainerUpdateProto.Builder builder = null;
	 boolean viaProto = false;
	
	 private ContainerId containerId;
	 
	 private Set<Integer> cpuCores;
	 
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
		 
		 if(this.cpuCores !=null){
			 builder.clearCpuCores();
			 builder.addAllCpuCores(cpuCores);
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
	public void setcpuCores(Set<Integer> cpuCores) {
		maybeInitBuilder();
		if (cpuCores == null) 
		      builder.clearCpuCores();
		this.cpuCores = cpuCores;
	}

	@Override
	public Set<Integer> getCpuCores() {
		NodeContainerUpdateProtoOrBuilder p = viaProto ? proto : builder;
	    if (this.cpuCores != null) {
		      return this.cpuCores;
		}
	    
	    if(p.getCpuCoresList()==null){
	    	
	    	return null;
	    }
	    this.cpuCores = new HashSet<Integer>();
	    this.cpuCores.addAll(p.getCpuCoresList());
		return this.cpuCores;
	}
	
    private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
		    return new ContainerIdPBImpl(p);
    }

    private ContainerIdProto convertToProtoFormat(ContainerId t) {
		    return ((ContainerIdPBImpl)t).getProto();
	}

}
