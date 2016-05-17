package org.apache.hadoop.examples;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
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
 * Map reads input data containing each line of the format <word|file n> showing how many times (n)
 * each word appears in each file.
 * Map separates count from the rest of the data in the input. Map output format is <word,<file,n>>. 
 * Reduce emits <word, list<n1, file1>, <n2, file2> ... > in decreasing order of occurrence of the word
 * in the respective files.
 *
 * 
 * To run: bin/hadoop jar build/hadoop-examples.jar rankedinvertedindex
 *            [-m <i>maps</i>] [-r <i>reduces</i>] <i>in-dir</i> <i>out-dir</i> 
 *
 * @author Faraz Ahmad 
 */


@SuppressWarnings("deprecation")
public class RankedInvertedIndex extends Configured implements Tool{

  private enum Counter { WORDS, VALUES }

  public static final Log LOG = LogFactory.getLog("org.apache.hadoop.examples.RankedInvertedIndex");

  public static class MapClass extends Mapper<LongWritable, Text, Text, FileCountPair> {

    public void map(LongWritable key, Text value, 
                   Context context) throws IOException,InterruptedException {

      String wordString = new String();
      String valueString = new String();
      String line = new String();
      String docId = new String();
      int countIndex;
      int count;
      int fileIndex;
      FileCountPair fileCountPair= new FileCountPair();

      line = ((Text)value).toString();

      // extract the count from the input string
      countIndex = line.lastIndexOf("\t");
      valueString = line.substring(0, countIndex);
      count = Integer.parseInt(line.substring(countIndex+1));

      // extract words and filename from the valueString 
      fileIndex = valueString.lastIndexOf("|");
      wordString = valueString.substring(0, fileIndex);
      docId = valueString.substring(fileIndex+1);

      fileCountPair = new FileCountPair(docId,count);
      context.write(new Text(wordString), fileCountPair);
    }
  }

  public static class Reduce extends Reducer<Text, FileCountPair, Text, Text> {

    public void reduce(Text key, Iterator<FileCountPair> values,
                      Context context) throws IOException,InterruptedException {

      int count,size;
      String docId = new String("");
      Text pair = new Text();
      FileCountPair valuePair, valueListArr[];
      List<FileCountPair> valueList= new ArrayList<FileCountPair>();

      while (values.hasNext()) {
        valuePair = new FileCountPair((FileCountPair) values.next());
        valueList.add(valuePair);
      }

      size = valueList.size();
      valueListArr = new FileCountPair[size];
      valueList.toArray(valueListArr);
      Arrays.sort(valueListArr);

      for (int i = 0; i < size; i++){
        count = valueListArr[i].getCount();
        docId = valueListArr[i].getFile();
        pair = new Text(count + "|" + docId); 
        context.write(key, pair);
        context.getCounter(Counter.VALUES).increment(1);
      }
    }
  }


  static void printUsage() {
    System.out.println("rankedinvertedindex [-m <maps>] [-r <reduces>] <input> <output>");
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
    Job job=Job.getInstance(conf,"RankedInvertedIndex");
    
    job.setJobName("rankedinvertedindex");
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(FileCountPair.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(MapClass.class);        
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
    int ret = ToolRunner.run(new RankedInvertedIndex(), args);
    System.exit(ret);
  }

}
