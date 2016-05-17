package org.apahce.hadoop.mapreduce.v2.app.ganglia;

import java.util.List;

import org.dom4j.Attribute;
import org.dom4j.Element;

class Metric implements ElementParse{
	
     String name;
    
	 String value;
    
     String type;
    
    public Metric(){
   	 
   	 this.name="";
   	 this.value="";
   	 this.type="";
    }
    
    public void Parse(Element node){
   	 
   	 if(node.getName() != "METRIC"){
				
		 return;		
	  }
		 List<Attribute> arrtibuteList = node.attributes();
		 
		 //first initialize or update attributes
		 for(Attribute attribute : arrtibuteList){
		    	
		    if(attribute.getName() == "NAME"){
		    	this.setName(attribute.getValue());
		    }
		    	
		    if(attribute.getName()=="TYPE"){
		    	this.setType(attribute.getValue());
		    }
		    
		    if(attribute.getName()=="VAL"){
		        this.setValue(attribute.getValue()); 
		    }
		 }
		    
    } 
		
	  
   public String getType() {
		return type;
	}


	public void setType(String type) {
		this.type = type;
	}

	public String getValue() {
			
		 return value;
	 }

	public void setValue(String value) {
			
		 this.value = value;
		
	 }
	
	public String getName() {
			
		 return name;
	}
	
	public void setName(String name) {
			this.name = name;
	}	
	
}

	
