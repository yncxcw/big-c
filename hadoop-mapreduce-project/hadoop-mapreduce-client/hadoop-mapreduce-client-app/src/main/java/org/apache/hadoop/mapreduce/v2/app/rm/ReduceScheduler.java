package org.apache.hadoop.mapreduce.v2.app.rm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.app.rm.RMContainerRequestor.ContainerRequest;
import org.apahce.hadoop.mapreduce.v2.app.ganglia.GangliaMonitor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class ReduceScheduler extends GangliaMonitor {
	
	static final Log LOG = LogFactory.getLog(ReduceScheduler.class);	

	@Override
	protected void serviceInit(Configuration conf) throws Exception {
		// TODO Auto-generated method stub
		
		LOG.info("ReduceScheduler Init");
		
		super.serviceInit(conf);
		
	
	}

	@Override
	protected void serviceStart() throws Exception {
		// TODO Auto-generated method stub
		super.serviceStart();
	}

	@Override
	protected void serviceStop() throws Exception {
		// TODO Auto-generated method stub
		super.serviceStop();
	}

	public ReduceScheduler(String name) {
		super(name);
		// TODO Auto-generated constructor stub
	}
	
	public abstract void selectHostForReduceRequest(ContainerRequest request);
		
		

}
