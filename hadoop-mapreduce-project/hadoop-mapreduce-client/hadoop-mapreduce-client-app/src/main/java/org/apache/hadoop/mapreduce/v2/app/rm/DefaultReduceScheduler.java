package org.apache.hadoop.mapreduce.v2.app.rm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.app.rm.RMContainerRequestor.ContainerRequest;
import org.apahce.hadoop.mapreduce.v2.app.ganglia.GangliaMonitor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DefaultReduceScheduler extends ReduceScheduler {
	
	static final Log LOG = LogFactory.getLog(DefaultReduceScheduler.class);	
	

	public DefaultReduceScheduler(String name) {
		super(name);
		// TODO Auto-generated constructor stub
	}
	
	

	@Override
	public void selectHostForReduceRequest(ContainerRequest request) {
		 
		  ArrayList<String> hosts = new ArrayList<String>(this.getCurrentSortedMetricByName("mem_free"));
		  
		  LOG.info("largest free memory host is:"+hosts.get(0));		  

	}

}
