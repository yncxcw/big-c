package org.apache.hadoop.yarn.server.nodemanager;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ContainerId;

public class CoresManagerImpl implements CoresManager {
	
	private Set<Integer> totalCores = new HashSet<Integer>();
	
	private Set<Integer> unUsedCores = new HashSet<Integer>();
		
	private Map<Integer,Set<ContainerId>> coresToContainer = new HashMap<Integer,Set<ContainerId>>();
    
	private Map<ContainerId, Set<Integer>> containerToCores = new HashMap<ContainerId, Set<Integer>>();

	@Override
	public void init() {
	// TODO read from configuration file
		
	}

	@Override
	public Set<Integer> getAvailableCores(ContainerId cntId, int num) {
		
		Set<Integer> returnedResults = new HashSet<Integer>();
		int index = 0;
		
		if(unUsedCores.size() > 0){
			for(Integer core : unUsedCores){
				returnedResults.add(core);
				index++;
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
		     }
		  }
		returnedResults.add(value);
		}
		
		for(Integer core : returnedResults){
			if(coresToContainer.get(core) == null ){
			    Set<ContainerId> containerIds = new HashSet<ContainerId>();
			    containerIds.add(cntId);
			    coresToContainer.put(core, containerIds);
		    }else{
		    	coresToContainer.get(core).add(cntId);
		    }
		}
		containerToCores.put(cntId, returnedResults);
		return returnedResults;
	}

	@Override
	public void releaseCores(ContainerId cntId) {
		
		Set<Integer> cores = containerToCores.get(cntId);
		for(Integer core : cores){
			coresToContainer.get(core).remove(cntId);
			if(coresToContainer.get(core).size() == 0){
				unUsedCores.add(core);
			}
		}
		containerToCores.remove(cntId);	
	}

}
