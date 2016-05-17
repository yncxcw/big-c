package org.apahce.hadoop.mapreduce.v2.app.ganglia;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.dom4j.Attribute;
import org.dom4j.Element;

class Cluster implements ElementParse{
	
	String name;
	
    String localTime;
    
	String owner;
	
	String latLong;
	
	String url;
	
	Map<String,Host> nameToHost;
	
	public Cluster(){
		
		this.name = "";
		this.localTime = "";
		this.owner="";
		this.latLong="";
		this.nameToHost = new HashMap<String,Host>();
	}
	
	public synchronized void Parse(Element node){
		
        if(node.getName() != "CLUSTER"){
			
			
		}
		
	    List<Attribute> arrtibuteList = node.attributes();
	    
	    //first initialize or update attributes
	    for(Attribute attribute : arrtibuteList){
	    	
	    	if(attribute.getName() == "NAME"){
	    		this.setName(attribute.getValue());
	    	}
	    	
	    	if(attribute.getName()=="OWNER"){
	    		this.setOwner(attribute.getValue());
	    	}
	    	
	    	if(attribute.getName()=="LOCALTIME"){
	    		this.setLocalTime(attribute.getValue());
	    	}
	    	
	    	if(attribute.getName() == "LATLONG"){
	    		this.setLatLong(attribute.getValue());
	    	}
	    	if(attribute.getName()=="URL"){
	    		
	    		this.setUrl(attribute.getValue());
	    	}
	    }
	    
	    //then parse the child nodes
	    
	    for (Iterator iterator = node.elementIterator( "HOST" ); iterator.hasNext(); ) {
             
	    	 Element element = (Element) iterator.next();
             
             String name = element.attribute("NAME").getValue();
             
             Host host= new Host();
             
             host.Parse(element);
             
             this.nameToHost.put(name, host);
                
	    }
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getLocalTime() {
		return localTime;
	}

	public void setLocalTime(String localTime) {
		this.localTime = localTime;
	}

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public String getLatLong() {
		return latLong;
	}

	public void setLatLong(String latLong) {
		this.latLong = latLong;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}
	
	public Metric getMatricOnHost(String host, String name){
		
		return this.nameToHost.get(host).getMetricByName(name);
	}
	
	public Host getHostByName(String host){
		
		return this.nameToHost.get(host);
	}
	
	public int getHostsNumber(){
		
		return this.nameToHost.size();
	}
	
	public Map<String,Host> getNameToHost(){
		
		return this.nameToHost;
	}
    

	
}