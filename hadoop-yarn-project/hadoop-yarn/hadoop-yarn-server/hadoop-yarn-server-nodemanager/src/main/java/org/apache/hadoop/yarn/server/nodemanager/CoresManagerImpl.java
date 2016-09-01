package org.apache.hadoop.yarn.server.nodemanager;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CoresManagerImpl implements CoresManager {
	
	 private static final Log LOG = LogFactory
		      .getLog(CoresManagerImpl.class);
	
	private Set<Integer> totalCores = new HashSet<Integer>();
	
	private Set<Integer> unUsedCores = new HashSet<Integer>();
	
	//should be initialized at start
	private Map<Integer,Set<ContainerId>> coresToContainer = new HashMap<Integer,Set<ContainerId>>();
    
	private Map<ContainerId, Set<Integer>> containerToCores = new HashMap<ContainerId, Set<Integer>>();

	@Override
	public void init(Configuration conf) {
        //we get cores fisrt		
		int virtualCores =
		        conf.getInt(
		            YarnConfiguration.NM_VCORES, YarnConfiguration.DEFAULT_NM_VCORES);

         //we initialize the total cores and unused cores
		for(int i=0;i<virtualCores;i++){
			totalCores.add(i);
			unUsedCores.add(i);
			Set<ContainerId> cntIdSet = new HashSet<ContainerId>();
			coresToContainer.put(i, cntIdSet);
		}
	}

	
	private Set<Integer> getAvailableCores(int num) {
		Set<Integer> returnedResults = new HashSet<Integer>();
		int index = 0;
		assert(num <= totalCores.size());
		
	    if(unUsedCores.size() > 0){
			for(Integer core : unUsedCores){
				returnedResults.add(core);
				index++;
				if(index >= num){
					break;
				}
			}
		}
			
		while(index < num){
	       Integer value = 0;
		   int     min = Integer.MAX_VALUE;
			   
		   for(Map.Entry<Integer, Set<ContainerId>> entry: coresToContainer.entrySet()){
			//find min core each time
		     if(returnedResults.contains(entry.getKey())){
		    	 continue;
		     }
		     
		     if(entry.getValue().size() < min){
		    	 value = entry.getKey();
		    	 min   = entry.getValue().size();
		     }
		  }
		returnedResults.add(value);
		index++;
		}
		
		return returnedResults;
	}
	
	@Override
	public synchronized Set<Integer> allocateCores(ContainerId cntId, int num){
		
		Set<Integer> returnedResults = this.getAvailableCores(num);
		this.allcoateCoresforContainer(returnedResults, cntId);
		LogOverlapWarning();
		
		return returnedResults;
	}
	
	private void allcoateCoresforContainer(Set<Integer> cores,ContainerId cntId){
		
		LOG.info("allocate cores: "+cores+" on container "+cntId);
		
		for(Integer core : cores){
			 unUsedCores.remove(core);
		 }
		 
		for(Integer core : cores){
		     coresToContainer.get(core).add(cntId);
		}

		if(containerToCores.get(cntId) == null){
		  //first allocated
		  containerToCores.put(cntId, cores);	
		}else{
		  //newly allocated cores
		   containerToCores.get(cntId).addAll(cores);
		}
		LOG.info("allocate cpuset "+cores);
	}

	@Override
	public synchronized void releaseCores(ContainerId cntId) {
		Set<Integer> cores= new HashSet<Integer>();
		
		if(containerToCores.get(cntId) == null){
			return;
		}
		cores.addAll(containerToCores.get(cntId));
		this.releaseCoresforContainer(cntId, cores);
		LogOverlapWarning();
		
	}
	
	private synchronized void releaseCoresforContainer(ContainerId cntId, Set<Integer> cores){
		
		LOG.info("release cores: "+cores+" on container "+cntId);
		
		for(Integer core : cores){
			if(coresToContainer.get(core).size() == 0){
				unUsedCores.add(core);
			}
		}
		
		for(Integer core : cores){
			coresToContainer.get(core).remove(cntId);
		}
		
		for(Integer core : cores){
			//remove core one by one
			containerToCores.get(cntId).remove(core);
		}
		
		//if there are no cores on this map, remove the entry
		if(containerToCores.get(cntId).size() == 0){
		    containerToCores.remove(cntId);
		}
		
		LOG.info("release cpuset "+cores);
	}
	
  @Override
  public synchronized Set<Integer> resetCores(ContainerId cntId, int num) {
		Set<Integer> cores = this.containerToCores.get(cntId);
		Set<Integer> returnedCores = new HashSet<Integer>();
		
	   //for a partially preempted container, its cores are null
	  if(cores != null){
	   if(num < cores.size()){
		//find the core that is used least
		for(int i=0; i<num; i++){
			int min = Integer.MAX_VALUE;
			Integer value = 0;
			for(Integer core : cores){
				if(returnedCores.contains(core)){
					continue;
				}
				
				if(coresToContainer.get(core).size() < min){
					value = core;
					min   = coresToContainer.get(core).size();
				}
			}
			returnedCores.add(value);
		}
		//remove cores to container mapping
		Set<Integer> toRemoved=new HashSet<Integer>();
		for(Integer core : cores){
			if(returnedCores.contains(core)){
				continue;
			}
			toRemoved.add(core);
		}
		
		this.releaseCoresforContainer(cntId, toRemoved);
					
	//for num >= cores.size(), we need to give more cores to this container
	}else{
	   returnedCores.addAll(cores);	
	   int required = num - cores.size();
	   if(required > 0){
		   Set<Integer> newAllocated = this.getAvailableCores(required);
		   returnedCores.addAll(newAllocated);
		   this.allcoateCoresforContainer(newAllocated, cntId);
	   }
	   
	}
 
 //for a fully preempted container
 }else{
	 Set<Integer> newAllocated = this.getAvailableCores(num);
	 returnedCores.addAll(newAllocated);
	 this.allcoateCoresforContainer(newAllocated, cntId);
 }
	
	LOG.info("get reset cores "+returnedCores);
	return returnedCores;
}
 
  private void LogOverlapWarning(){
	  for(Integer core : this.coresToContainer.keySet()){
		  
		  if(this.coresToContainer.get(core).size() > 1){
			  LOG.info("cpuset overlap warning on core"+core+"size:"+this.coresToContainer.get(core).size());
			  LOG.info("cores: "+ core + "containers:"+this.coresToContainer.get(core));
		  }
	  }
	  
  }

}
