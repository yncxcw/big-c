package org.apahce.hadoop.mapreduce.v2.app.ganglia;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;


public class MetricsRetrieve{
	
	 /*
	  * object to store and parse.
	  */
	 Grid grid;
	 
	 long currentTime;
	 
	 public void MetricsRetrive(){
		 
		 currentTime = System.currentTimeMillis();
	 }
	 
	 
	 public void Parse(String input){
		 
		Document document = null;
		 
		if(grid == null){
			
			grid = new Grid();
		}
		
		currentTime=System.currentTimeMillis();
		
		try {
            
			document = DocumentHelper.parseText(input);
        
		} catch (DocumentException e) {
            //解析失败
            e.printStackTrace();
        }
		
		Element rootNode = document.getRootElement();
		
		if(rootNode != null){
			
		   Iterator iterator = rootNode.elementIterator( "GRID" );
			
		   grid.Parse((Element)iterator.next());
		
		}
	   	 	 
	 }
	 
	 public Cluster getClusterByName(String name){
		 
		 return this.grid.getClusterByName(name);
	 }
	 
	 public Host getHostByNameInCluster(String cluster, String host){
		 
		 return this.grid.getHostinClusterByName(cluster, host);
	 }
	 
	 public Metric getMetricByNameInClusterOnHost(String cluster, String host,String name){
		 
		 return this.grid.getHostinClusterByName(cluster, host).getMetricByName(name);
		 
	 }
	 
	 public List<Metric> getMetricsByNameInCluster(String cluster, String host){
		 
		 return this.grid.getClusterByName(cluster).getHostByName(host).getMetrics();
	 }
	 
	 public int getClusterNumber()
	 {
		 return this.grid.getClusterNumber();
	 }
	 
	 public int getClusterNodeNumber(String cluster){
		 
		 return this.grid.getClusterByName(cluster).getHostsNumber();
	 }
	

	
	public static void main(String [] args){
		
		RemoteReader httpAccess = new RemoteReader();
		
		String input =httpAccess.SendRequest();
		
		MetricsRetrieve metricsRetrive = new MetricsRetrieve();
		
		metricsRetrive.Parse(input);
		
		System.out.print(metricsRetrive.getClusterNumber());
		
		List<Metric> metrics = metricsRetrive.getMetricsByNameInCluster("jassion", "localhost");
		
		for(Metric metric  : metrics){
			
			System.out.println("name:  "+metric.getName());
			
			System.out.println("value: "+metric.getValue());
			
			System.out.println("type: "+metric.getType());
			
		}
		
		return ;
		
	}

	
	
	
	

}
