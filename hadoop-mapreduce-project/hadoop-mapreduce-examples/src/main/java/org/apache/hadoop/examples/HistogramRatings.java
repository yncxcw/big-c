package org.apache.hadoop.examples;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This is an example Hadoop Map/Reduce application.
 * It reads the input movie files and outputs a histogram showing how many reviews fall in which range
 * We make 5 reduce tasks for ratings of 1, 2, 3, 4, and 5.
 * the reduce task counts all reviews in the same bin and emits them.
 *
 * To run: bin/hadoop jar build/hadoop-examples.jar histogram_ratings
 *            [-r <i>reduces</i>] <i>in-dir</i> <i>out-dir</i> 
 *
 * @author Faraz Ahmad modified by Wei Chen
 */
@SuppressWarnings("deprecation")
public class HistogramRatings extends Configured implements Tool{

  private enum Counter { WORDS, VALUES }

  public static final Log LOG = LogFactory.getLog("org.apache.hadoop.examples.HistogramRatings");

  public static class MyMapClass extends Mapper<Object, Text, IntWritable, IntWritable> {

    private final static IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, 
        Context context) throws IOException, InterruptedException{

      int rating, reviewIndex, movieIndex;
      String reviews = new String();
      String tok = new String();
      String ratingStr = new String();

      String line = ((Text)value).toString();
      movieIndex = line.indexOf(":");
      if (movieIndex > 0) {
        reviews = line.substring(movieIndex + 1);
        StringTokenizer token = new StringTokenizer(reviews, ",");
        while (token.hasMoreTokens()) {
          tok = token.nextToken();
          reviewIndex = tok.indexOf("_");
          ratingStr = tok.substring(reviewIndex + 1);
          rating = Integer.parseInt(ratingStr);
          context.write(new IntWritable(rating), one);
        }
      }
    }
  }

  public static class MyReduceClass 
    extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context
            ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
          sum += val.get();
      }
      context.write(key, new IntWritable(sum));
    }

  }


  static void printUsage() {
    System.out.println("histogram_ratings [-r <reduces>] <input> <output>");
    System.exit(1);
  }


  /**
   * The main driver for histogram_ratings map/reduce program.
   * Invoke this method to submit the map/reduce job.
   * @throws IOException When there is communication problems with the 
   *                     job tracker.
   */

  public int run(String[] args) throws Exception {
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "histogram_ratings");
	job.setJarByClass(HistogramRatings.class);
    job.setJobName("histogram_ratings");  
    job.setMapperClass(MyMapClass.class);        
    job.setCombinerClass(MyReduceClass.class);
    job.setReducerClass(MyReduceClass.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);

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
    return waitforCompletion ? 0 : 1;
  }
  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new HistogramRatings(), args);
    System.exit(ret==0 ? 0 : 1);
   
  }
}
