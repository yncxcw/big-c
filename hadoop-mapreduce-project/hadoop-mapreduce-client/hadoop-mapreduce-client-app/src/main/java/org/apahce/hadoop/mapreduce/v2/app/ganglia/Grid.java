package org.apahce.hadoop.mapreduce.v2.app.ganglia;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.dom4j.Attribute;
import org.dom4j.Element;

class Grid{
	
	String name;
	
	String autority;
	
	String localTime;
	
	Map<String,Cluster> nameToClusters;
	
	public Grid(){
		
		name ="";
		autority="";
		nameToClusters= new HashMap<String,Cluster>();
		
	}
	
	void Parse(Element node){
		
		if(node.getName() != "GRID"){
			
		}
		
	    List<Attribute> arrtibuteList = node.attributes();
	    
	    //first initialize or update attributes
	    for(Attribute attribute : arrtibuteList){
	    	
	    	if(attribute.getName() == "NAME"){
	    		this.setName(attribute.getValue());
	    	}
	    	
	    	if(attribute.getName()=="AUTHORITY"){
	    		this.setAutority(attribute.getValue());
	    	}
	    	
	    	if(attribute.getName()=="LOCALTIME"){
	    		this.setLocalTime(attribute.getValue());
	    	}
	    }
	    
	    //then parse the child nodes
	    
	    for (Iterator iterator = node.elementIterator( "CLUSTER" ); iterator.hasNext(); ) {
             Element element = (Element) iterator.next();
             
             String name = element.attribute("NAME").getValue();
             
             System.out.println("cluster name"+name);
             
             Cluster cluster= new Cluster();
             
             cluster.Parse(element);
             
             this.nameToClusters.put(name, cluster);
                
        }
					
	}
	

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getAutority() {
		return autority;
	}

	public void setAutority(String autority) {
		this.autority = autority;
	}

	public String getLocalTime() {
		return localTime;
	}

	public void setLocalTime(String localTime) {
		this.localTime = localTime;
	}
	
	public Cluster getClusterByName(String name){
		
		return this.nameToClusters.get(name);
	}
	
	public Host getHostinClusterByName(String cluster, String host){
		
		return this.nameToClusters.get(cluster).getHostByName(host);
	}
	
	public int getClusterNumber(){
		
		return this.nameToClusters.size();
	}
}






