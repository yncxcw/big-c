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
			
			for(Integer core : returnedResults){
				unUsedCores.remove(core);
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
		}
		
		return returnedResults;
	}
	
	@Override
	public Set<Integer> allocateCores(ContainerId cntId, int num){
		LogOverlapWarning();
		Set<Integer> returnedResults = this.getAvailableCores(num);
		this.allcoateCoresforContainer(returnedResults, cntId);
		
		return returnedResults;
	}
	
	private void allcoateCoresforContainer(Set<Integer> cores,ContainerId cntId){
		for(Integer core : cores){
		    coresToContainer.get(core).add(cntId);
		}
		containerToCores.put(cntId, cores);	
		LOG.info("allocate cpuset "+cores);
	}

	@Override
	public void releaseCores(ContainerId cntId) {
		LogOverlapWarning();
		Set<Integer> cores= containerToCores.get(cntId);
		this.releaseCoresforContainer(cntId, cores);
	}
	
	private void releaseCoresforContainer(ContainerId cntId, Set<Integer> cores){
		for(Integer core : cores){
			coresToContainer.get(core).remove(cntId);
			if(coresToContainer.get(core).size() == 0){
				unUsedCores.add(core);
			}
		}
		containerToCores.remove(cntId);
		LOG.info("release cpuset "+cores);
	}
	
  @Override
  public Set<Integer> resetCores(ContainerId cntId, int num) {
		Set<Integer> cores = this.containerToCores.get(cntId);
		Set<Integer> returnedCores = new HashSet<Integer>();
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
	
	return returnedCores;
}
 
  private void LogOverlapWarning(){
	  LOG.info("cpuset warning");
	  for(Integer core : this.coresToContainer.keySet()){
		  if(this.coresToContainer.get(core).size() > 1){
			  LOG.info("cpuset overlap warning on core"+core+"size:"+this.coresToContainer.get(core).size());
		  }
	  }
	  
  }

}
