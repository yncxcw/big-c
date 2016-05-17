package org.apache.hadoop.examples;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * This is an example Hadoop Map/Reduce application.
 * It reads the text input files each line of the format <A1;A2;.....;A12>, and produces <<A1;A2;..;A11>,A12> 
 * as Map output.
 * The Reduce output is a list of all candidates for the self join common items 
 * 
 *
 * To run: bin/hadoop jar build/hadoop-examples.jar selfjoin
 *            [-m <i>maps</i>] [-r <i>reduces</i>] <i>in-dir</i> <i>out-dir</i> 
 *
 * @author Faraz Ahmad
 */

@SuppressWarnings("deprecation")
public class SelfJoin extends Configured implements Tool{

  private enum Counter { WORDS, VALUES }

  public static final Log LOG = LogFactory.getLog("org.apache.hadoop.examples.SelfJoin");

  public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, 
                    Context context) throws IOException,InterruptedException {

      String line = new String();
      String kMinusOne = new String();
      String kthItem = new String();
      int index;

      line = ((Text)value).toString();
      index = line.lastIndexOf(",");
      if(index == -1) {
        LOG.info("Map: Input File in wrong format");
        return;
      }
      kMinusOne = line.substring(0,index);
      kthItem = line.substring(index+1);

      context.write(new Text(kMinusOne), new Text(kthItem));
      context.getCounter(Counter.WORDS).increment(1);
    }
  }

  /**
   * A reducer class that makes combinations for candidates.
   */
  public static class Reduce extends Reducer<Text, Text, Text, Text> {


    public void reduce(Text key, Iterator<Text> values,
                      Context context) throws IOException,InterruptedException {

      String value = new String("");
      String outVal = new String ("");
      List<String> kthItemList = new ArrayList<String>();

      while (values.hasNext()){
        value = ((Text) values.next()).toString();
        kthItemList.add(value);
      }
      Collections.sort(kthItemList);
      for (int i = 0; i < kthItemList.size() - 1; i++){
        for (int j = i+1; j < kthItemList.size(); j++) {
          outVal = kthItemList.get(i) + "," + kthItemList.get(j);
          context.write(key,new Text(outVal));
        }
      }
    }
  }

  /**
   * A combiner class that removes the duplicates in the map output.
   */
  public static class Combine extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterator<Text> values,
                       Context context) throws IOException,InterruptedException {

      String value = new String("");
      List<String> kthItemList = new ArrayList<String>();

      while (values.hasNext()){
        value = ((Text) values.next()).toString();
         kthItemList.add(value);
      }
      for (int i = 0; i < kthItemList.size(); i++){
        context.write(key,new Text(kthItemList.get(i)));
      }
    }
  }

  static void printUsage() {
    System.out.println("selfjoin [-m <maps>] [-r <reduces>] <input> <output>");
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
    Job job = Job.getInstance(conf, "SelfJoin");
    job.setJobName("selfjoin");
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setMapperClass(MapClass.class);        
    job.setCombinerClass(Combine.class);
    job.setReducerClass(Reduce.class);

    List<String> other_args = new ArrayList<String>();
    for(int i=0; i < args.length; ++i) {
      try {
        if ("-m".equals(args[i])) {
          
        } else if ("-r".equals(args[i])) {
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
    return 0;
  }
  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new SelfJoin(), args);
    System.exit(ret);
  }
}
