package org.apache.hadoop.examples.hdfs;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HdfsWrite {
         public static void main(String[] args) throws IOException{
        	 
        	  Path path= new Path("hdfs://192.168.0.21:9000/hdfs/writefile");
        	  FileSystem fs = FileSystem.get(new Configuration());
        	  
        	  BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fs.create(path, true)));
        	  
        	  String line;
        	  
        	  line = "first hdfs write example";
        	  
        	  System.out.print(line);
        	  
        	  bufferedWriter.write(line);
        	  
        	  bufferedWriter.close();
        	               	 
         }
	 
}
