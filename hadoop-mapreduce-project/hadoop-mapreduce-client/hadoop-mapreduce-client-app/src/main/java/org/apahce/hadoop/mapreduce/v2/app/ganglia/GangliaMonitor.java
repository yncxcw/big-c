package org.apahce.hadoop.mapreduce.v2.app.ganglia;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.app.rm.RMContainerAllocator;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GangliaMonitor extends AbstractService{
	
   static final Log LOG = LogFactory.getLog(GangliaMonitor.class);	
   
   public static final String DEFAULT_GANGLIA_SERVER_HOST = "127.0.0.1"; 
 	
   private final int UPDATE_INTERVAL;
	
   private final AtomicBoolean stopped;
      
   private Object lock_read = new Object();
   
   private Thread  loopPullThread;
   
   private RemoteReader remoteReader;
   
   private MetricsRetrieve metricsRetrive;
   
   private String clusterName       = "DISCO_CLUSTER";
   
   private int remainingRetries     = 10;
   
   private int hostMetricsListLimit = 10;
   
   private Set<String> registeredMetrics;
   
   private final String defaultCPUMetrics[]   ={"cpu_speed","cpu_num","cpu_wio","cpu_user","cpu_idle","cpu_system"};
   
   private final String defulatMemoryMetrics[]={"mem_total","mem_free","swap_total"};
   
   private final String defaultDiskMetrics[]  ={"disk_free","disk_total"};
   
   private final String defaultNetworkMetrics[]={"bytes_out","bytes_in","pkts_in","pkts_out"};
   
   
   
   private LinkedList<Map<String,Map<String,GangliaMetric>>> hostMetricsList;
	
	public GangliaMonitor(String name) {
		
		super(name);
		this.UPDATE_INTERVAL = 3000;
		this.stopped          = new AtomicBoolean(false);
		this.remoteReader     = new RemoteReader();
		this.metricsRetrive   = new MetricsRetrieve();
		this.hostMetricsList  = new LinkedList<Map<String,Map<String,GangliaMetric>>>();
		this.registeredMetrics= new HashSet<String>();
		
		
		
		// TODO Auto-generated constructor stub
	}

	@Override
	protected void serviceInit(Configuration conf) throws Exception {
		// TODO Auto-generated method stub
		String gangliaServerHost = conf.getStrings(MRJobConfig.GANGLIA_SERVER_HOST, DEFAULT_GANGLIA_SERVER_HOST)[0];
		
		LOG.info("server host is  "+gangliaServerHost);
		
		this.remoteReader.setUrl(gangliaServerHost);
		
		//add cpu default metrics
		for(String metric :defaultCPUMetrics){
			
			this.registeredMetrics.add(metric);
		}
		//add memory default metrics
		for(String metric :defulatMemoryMetrics){
			
			this.registeredMetrics.add(metric);
		}
		//add disk default metrics
		for(String metric :defaultDiskMetrics){
			
			this.registeredMetrics.add(metric);
		}
		//add network defulat metrics
		for(String metric :defaultNetworkMetrics){
			
			this.registeredMetrics.add(metric);
		}
		
		super.serviceInit(conf);
	}

	@Override
	protected void serviceStart() throws Exception {
		// TODO Auto-generated method stub
		
		this.loopPullThread = new Thread(){
			public void run(){	
			   while(!stopped.get() && !Thread.currentThread().isInterrupted()){  	  
				   try{
				        Thread.sleep(UPDATE_INTERVAL);
					    
				        String gangliaStr = remoteReader.SendRequest();
				        
					    synchronized(lock_read){
					    	metricsRetrive.Parse(gangliaStr);
					    	Cluster cluster = metricsRetrive.getClusterByName(clusterName);
					    	Map<String,Map<String,GangliaMetric>> mapHostToMetrics = new HashMap<String,Map<String,GangliaMetric>>();
					    	for(Map.Entry<String, Host> entryPair : cluster.getNameToHost().entrySet()){
					    	   	Map<String,GangliaMetric> metrics = new HashMap<String,GangliaMetric>(); 
					    		String hostName = entryPair.getKey();
					    		for(Metric metric : entryPair.getValue().getMetrics()){
					    		if(registeredMetrics.contains(metric.getName())){
					    			GangliaMetric gangliaMetric = new GangliaMetric();
					    			gangliaMetric.name = metric.getName();
					    			gangliaMetric.type = metric.getType();
					    			gangliaMetric.value= metric.getValue();
					    			metrics.put(gangliaMetric.name, gangliaMetric);
					    			
					    		  }else{  
					    			continue;
					    		  }
					    		}
					    	 mapHostToMetrics.put(hostName, metrics);
					        }
					    	
					    	if(hostMetricsList.size() < hostMetricsListLimit){
					    		
					    		hostMetricsList.add(mapHostToMetrics);
					    	
					    	}else{
					    	 	
					    	    hostMetricsList.remove();
					    	    hostMetricsList.add(mapHostToMetrics);
					    	}
					    	
					    	
					   }
				   
				 }catch(Throwable t){
				 
					LOG.info("Communication exception: "+ StringUtils.stringifyException(t));
				    remainingRetries--;
				    if(remainingRetries < 0){
				    	
				    	
				    }
					    
				  }   
			   }	 	
			}	
		};
		
		this.loopPullThread.start();
		super.serviceStart();
	}

	public Map<String,Map<String,GangliaMetric>> getCurrentClusterMetric(){
		
	    if(this.hostMetricsList!=null){
	        synchronized(lock_read){
	    
	    		return this.hostMetricsList.getLast();
	     }
	    }else{
	    	
	    	return null;
	    }	
	 
	}
	
	public List<Map<String,Map<String,GangliaMetric>>> getRecentNClusterMetric(int N){
				
		if(N > this.hostMetricsList.size()){
		 
			N = this.hostMetricsList.size();
			
		}
		List<Map<String,Map<String,GangliaMetric>>> reurnValues = new LinkedList<Map<String,Map<String,GangliaMetric>>>();	
		synchronized(lock_read){
			for(int i=this.hostMetricsList.size()-1;i >= this.hostMetricsList.size()- N;i--){	
				Map<String,Map<String,GangliaMetric>> value = this.hostMetricsList.get(i);
				reurnValues.add(value);
			}	
		}
		
	  return reurnValues;	
	}
	
	public List<String> getCurrentSortedMetricByName(String metric){
		
		if(!registeredMetrics.contains(metric))
		{
			return null;
		}		
		 if(this.hostMetricsList!=null){
			 Map<String,Map<String,GangliaMetric>> currentMetrics;
			 Map<String, Double> currentMetricsByName;
			 synchronized(lock_read){
				currentMetrics = hostMetricsList.getLast();
				currentMetricsByName = new HashMap<String,Double>();
				 
				 for(String host: currentMetrics.keySet()){
					  double value = currentMetrics.get(host).get(metric).toDouble();
					  currentMetricsByName.put(host, value);
				 }
				
				List<String> sortedMetrics = sortByValue(currentMetricsByName);
				
				return sortedMetrics;
			 }
		   
		 }else{
			 
			 return null;
		 }
				
	}
	
	public static <K, V extends Comparable<? super V>> List<K> 
    sortByValue( Map<K, V> map )
{
    List<Map.Entry<K, V>> list = new LinkedList<>( map.entrySet() );
    Collections.sort( list, new Comparator<Map.Entry<K, V>>()
    {
        @Override
        public int compare( Map.Entry<K, V> o1, Map.Entry<K, V> o2 )
        {
            return (o1.getValue()).compareTo( o2.getValue() );
        }
    } );

    List<K> result = new ArrayList<>();
    for (Map.Entry<K, V> entry : list)
    {
        result.add(entry.getKey());
    }
    return result;
}
	@Override
	protected void serviceStop() throws Exception {
	    if(this.stopped.getAndSet(true)){
	    	
	    	return;
	    }
	    
	    if(this.loopPullThread !=null ){
	    	
	    	this.loopPullThread.interrupt();
	    }
		// TODO Auto-generated method stub
		super.serviceStop();
	}

	
	
	class GangliaMetric{
		
		 public String name;
		    
		 public String value;
	    
	     public String type;
	     
	     public double toDouble(){
	    	 
	    	  if(isToDouble()){
	    		 
	    		  return Double.parseDouble(value);
	    	  
	    	  }else{
	    		  LOG.info("metric convert error can't convert to double");
	    		  return -1;
	    	  }
	     }
	     
	     public int toInt(){
	    	 
	    	 if(isToInt()){
	    		 
	    		 return Integer.parseInt(value);
	    	 
	    	 }else{
	    		 LOG.info("metric convert error can't convert to int");
	    		 return -1;
	    	 }
	    	 
	     }
	     
	     public boolean isToDouble(){
	    	 
	    	 return !(type=="string");
	     }
	     
	     public boolean isToInt(){
	    	 
	    	 return (type=="unit16")||(type=="unit32"); 	 
	     }
	}
	
			

}
