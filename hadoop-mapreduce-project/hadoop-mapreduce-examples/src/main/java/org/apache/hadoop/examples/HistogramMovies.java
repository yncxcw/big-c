package org.apache.hadoop.examples;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * This is an example Hadoop Map/Reduce application.
 * It reads the input movie files and outputs a histogram showing how many movies fall in which
 * category of reviews. 
 * We make 8 reduce tasks for 1-1.5,1.5-2,2-2.5,2.5-3,3-3.5,3.5-4,4-4.5,4.5-5.
 * the reduce task counts all reviews in the same bin.
 *
 * To run: bin/hadoop jar build/hadoop-examples.jar histogram_movies
 *            [-r <i>reduces</i>] <i>in-dir</i> <i>out-dir</i> 
 *
 * @author Faraz Ahmad
 */

@SuppressWarnings("deprecation")
public class HistogramMovies extends Configured implements Tool{

  private enum Counter { WORDS, VALUES }

  public static class MapClass extends Mapper<Object, Text, FloatWritable, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private final static float division = 0.5f;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      int rating, movieIndex, reviewIndex;
      int totalReviews = 0, sumRatings = 0; 
      float avgReview = 0.0f, absReview, fraction, outValue = 0.0f;
      String reviews = new String();
      String line = new String();
      String tok = new String();
      String ratingStr = new String();
      
      line = ((Text)value).toString();
      movieIndex = line.indexOf(":");
      if (movieIndex > 0) {
        reviews = line.substring(movieIndex + 1);
        StringTokenizer token = new StringTokenizer(reviews, ",");
        while (token.hasMoreTokens()) {
          tok = token.nextToken();
          reviewIndex = tok.indexOf("_");
          ratingStr = tok.substring(reviewIndex + 1);
          rating = Integer.parseInt(ratingStr);
          sumRatings += rating;
          totalReviews ++;
        }
        avgReview = (float) sumRatings / (float) totalReviews;
        absReview = (float) Math.floor((double)avgReview);

        fraction = avgReview - absReview;
        int limitInt = Math.round(1.0f/division);

        for (int i = 1; i <= limitInt; i++){
          if(fraction < (division * i) ){
            outValue = absReview + division * i;
            break;
          }
        }
        context.write(new FloatWritable(outValue), one);
      }
    }
  }

  public static class Reduce extends Reducer<FloatWritable, IntWritable, FloatWritable, IntWritable> {
    
    public void reduce(FloatWritable key, Iterable<IntWritable> values,
        Context context) throws IOException, InterruptedException {

    	int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }

  }

  static void printUsage() {
    System.out.println("histogram_movies [-r <reduces>] <input> <output>");
    System.exit(1);
  }

  /**
   * The main driver for histogram map/reduce program.
   * Invoke this method to submit the map/reduce job.
   * @throws IOException When there is communication problems with the 
   *                     job tracker.
   */

  public int run(String[] args) throws Exception {

	  Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "histogram_movies");
    job.setJobName("histogram_movies");
    job.setOutputKeyClass(FloatWritable.class);
    job.setOutputValueClass(IntWritable.class);

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
    if (other_args.size() < 2) {
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
    int ret = ToolRunner.run(new HistogramMovies(), args);
    System.exit(ret==0 ? 0 : 1);
    
  }
}
