package org.apahce.hadoop.mapreduce.v2.app.ganglia;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.dom4j.Attribute;
import org.dom4j.Element;

class Host implements ElementParse{
	
	String name;
	
	String ip;
	
	Map<String,Metric> nameToMetrics;
	
	public Host(){
		
		this.name = "";
		this.ip="";
		this.nameToMetrics = new HashMap<String,Metric>();
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
		    	
		  if(attribute.getName()=="IP"){
		    		this.setIp(attribute.getValue());
		}
	  }
		    
		    //then parse the child nodes
		    
	 for (Iterator iterator = node.elementIterator( "METRIC" ); iterator.hasNext(); ) {
	          Element element = (Element) iterator.next();
	             
	          String name = element.attribute("NAME").getValue();
	             
	          Metric metric= new Metric();
	             
	          metric.Parse(element);
	             
	          this.nameToMetrics.put(name, metric);
	                
		 }	
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}
	
	public Metric getMetricByName(String name){
		
		return nameToMetrics.get(name);
	}
	
	public List<Metric> getMetrics(){
		
		List<Metric> metrics = new LinkedList<Metric>();
		
		for(Map.Entry<String, Metric> metricPair: this.nameToMetrics.entrySet()){
			
			metrics.add(metricPair.getValue());
		}
		
		return metrics;
			
	}
}