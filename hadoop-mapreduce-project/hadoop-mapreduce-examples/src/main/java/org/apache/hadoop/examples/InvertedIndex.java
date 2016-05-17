package org.apache.hadoop.examples;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This is an example Hadoop Map/Reduce application.
 * It reads the text input files, breaks each line into words
 * Map emits <word, docid> with each word emitted once per document
 * Reduce takes map output and emits <word, list(docid)>
 *
 * To run: bin/hadoop jar build/hadoop-examples.jar invertedindex
 *           [-r <i>reduces</i>] <i>in-dir</i> <i>out-dir</i> 
 *
 * @author Faraz Ahmad
 */


@SuppressWarnings("deprecation")
public class InvertedIndex extends Configured implements Tool{

  private enum Counter { WORDS, VALUES } 
  public static final Log LOG = LogFactory.getLog("org.apache.hadoop.examples.InvertedIndex");

  /**
   * For each line of input, break the line into words and emit them as
   * (<b>word,doc</b>).
   */

  public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

   
    public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {

      String docName = new String("");
      Text docId, wordText;
      String line;
      
      FileSplit fileSplit = (FileSplit) context.getInputSplit();
      
      docName = fileSplit.getPath().getName();
      
      docId = new Text(docName);
      line = ((Text)value).toString();
      StringTokenizer itr = new StringTokenizer(line);
      while (itr.hasMoreTokens()) {
        wordText = new Text(itr.nextToken());
        context.write(wordText, docId);
      }
    }
  }
  /**
   * The reducer class 
   */
  public static class Reduce extends Reducer<Text, Text, Text, Text> {

    String str1 = new String();
    String str2 = new String();
    String valueString = new String("");
    Text docId;
    private HashSet<String> duplicateSet = new HashSet<String>();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	
       for(Text value: values) {
        
    	valueString =  value.toString();
        
        duplicateSet.add(valueString);
        
      }
       
     context.write(key, new Text(duplicateSet.toString()));
     
     
    }
  }


  static void printUsage() {
    System.out.println("invertedindex [-r <reduces>] <input> <output>");
    System.exit(1);
  }


  /**
   * The main driver for map/reduce program.
   * Invoke this method to submit the map/reduce job.
   * @throws IOException When there is communication problems with the 
   *                     job tracker.
   */

  public int run(String[] args) throws Exception {
	  
    Configuration conf = new Configuration(); 
    Job job = Job.getInstance(conf,"invertedindex");
    job.setJobName("invertedindex");
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setMapperClass(MapClass.class);        
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);

    List<String> other_args = new ArrayList<String>();
    for(int i=0; i < args.length; ++i) {
      try {
        if ("-r".equals(args[i])) {
          job.setNumReduceTasks(Integer.parseInt(args[++i]));
        } else {
          other_args.add(args[i]);
        }
      } catch (NumberFormatException except) {
        System.out.println("ERROR: Integer expected instead of " + args[i]);
        printUsage();
      } catch (ArrayIndexOutOfBoundsException except) {
        System.out.println("ERROR: Required parameter missing from " +
            args[i-1]);
        printUsage(); // exits
      }
    }
    // Make sure there are exactly 2 parameters left.
    if (other_args.size() != 2) {
      System.out.println("ERROR: Wrong number of parameters: " +
          other_args.size() + " instead of 2.");
      printUsage();
    }

    FileInputFormat.addInputPath(job, new Path(other_args.get(0)));
    String outPath = new String(other_args.get(1));
    FileOutputFormat.setOutputPath(job, new Path(outPath));

    Date startTime = new Date();
    System.out.println("Job started: " + startTime);

    Boolean waitforCompletion = job.waitForCompletion(true) ;

    Date end_time = new Date();
    System.out.println("Job ended: " + end_time);
    System.out.println("The job took " +
        (end_time.getTime() - startTime.getTime()) /1000 + " seconds.");
    System.exit(waitforCompletion ? 0 : 1);
    return 0;
  }
  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new InvertedIndex(), args);
  }

}
