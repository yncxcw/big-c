package org.apache.hadoop.examples;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * This is an example Hadoop Map/Reduce application.
 * It reads the text input files, breaks each line into words
 * Map output : <host, termVector> where host is extracted from filename,
 *     termVector is a string <word:1> for all words in the input file
 * Reduce: counts all occurrences of all words in each file and emits for each host,
 * all those words which occur above a threshold specified by CUTOFF.
 * output format: <host, list<termVectors>> 
 *
 * To run: bin/hadoop jar build/hadoop-examples.jar termvectorperhost
 *            [-r <i>reduces</i>] <i>in-dir</i> <i>out-dir</i> 
 *
 * @author Faraz Ahmad
 */

@SuppressWarnings("deprecation")
public class TermVectorPerHost extends Configured implements Tool{

  private enum Counter { WORDS, VALUES }

  public static final Log LOG = LogFactory.getLog("org.apache.hadoop.examples.TermVectorPerHost");
  public static final int CUTOFF = 10;


  public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

    private final static IntWritable one = new IntWritable(1);
    private String path;

    public void configure(JobConf conf){
      path = conf.get("map.input.file");
    }

    public void map(LongWritable key, Text value, 
        Context context) throws IOException, InterruptedException {

      String docName = new String("");
      String line = new String();
      String word = new String();
      Text host, termVector;

     FileSplit fileSplit = (FileSplit) context.getInputSplit();
      
      docName = fileSplit.getPath().getName();
      
      host = new Text(docName);
      line = ((Text)value).toString();

      StringTokenizer itr = new StringTokenizer(line);
      while (itr.hasMoreTokens()) {
        word = itr.nextToken();
        termVector = new Text(word + ":" + one);
        context.write(host, termVector);
      }
    }
  }

  /**
   * The combiner class
   */
  public static class Combine extends Reducer<Text, Text, Text, Text> {

	@Override
    public void reduce(Text key, Iterable<Text> values,
        Context context) throws IOException, InterruptedException {

      Map<String, IntWritable> vectorTerms= new HashMap<String, IntWritable>();
      String freqString = new String();
      String termVector = new String();
      String word = new String("");
      int freq = 0;
      String newTermVector = new String();

      for(Text value: values) {

        termVector = value.toString();
        int index = termVector.lastIndexOf(":");
        word      = termVector.substring(0, index);
        freqString = termVector.substring(index+1);
        freq = Integer.parseInt(freqString);
        if (vectorTerms.containsKey(word)){
          freq += vectorTerms.get(word).get();
        }
        vectorTerms.put(word, new IntWritable(freq));
      }
      Set<Map.Entry<String, IntWritable>> set = vectorTerms.entrySet();
      Iterator<Map.Entry<String, IntWritable>> i = set.iterator();
      while(i.hasNext()){
        Map.Entry<String, IntWritable> me = (Map.Entry<String, IntWritable>)i.next();
        newTermVector = new String(me.getKey() + ":" + me.getValue());
        context.write(key, new Text (newTermVector));
      }
    }
  }

  /**
   * A reducer class that adds term vectors, throws away infrequent terms (occuring less than CUTOFF times) 
    and emits a final (host,termvector) = (host,(word,frequency)) pair.
   */
  public static class Reduce extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values,
        Context context) throws IOException, InterruptedException  {

      Map<String, IntWritable> vectorTerms= new HashMap<String, IntWritable>();
      String freqString = new String("");
      String termVector = new String("");
      String word = new String("");
      int freq = 0;

      for(Text value: values){

    	termVector = value.toString();
    	 int index = termVector.lastIndexOf(":");
         word      = termVector.substring(0, index);
         freqString = termVector.substring(index+1);
        freq = Integer.parseInt(freqString);

        if (vectorTerms.containsKey(word)){
          freq += vectorTerms.get(word).get();
        }
        vectorTerms.put(word, new IntWritable(freq));
      }
      Map<String, IntWritable> vectorTermsSorted = sortByValue(vectorTerms);
      Set<Map.Entry<String, IntWritable>> set = vectorTermsSorted.entrySet();
      Iterator<Map.Entry<String, IntWritable>> i = set.iterator();
      while(i.hasNext()){
        Map.Entry<String, IntWritable> me = (Map.Entry<String, IntWritable>)i.next();
        if(me.getValue().get() >= CUTOFF){
          String termVectorString = new String(me.getKey() + ":" + me.getValue());
          context.write(key, new Text (termVectorString));
        }
      }
    }
    @SuppressWarnings("unchecked")
    static Map<String, IntWritable> sortByValue(Map<String, IntWritable> map) {
      List<Object> list = new LinkedList<Object>(map.entrySet());
      Collections.sort(list, new Comparator<Object>() {
        public int compare(Object o1, Object o2) {
          return -((IntWritable) ((Map.Entry<String, IntWritable>) (o1)).getValue())
          .compareTo((IntWritable) ((Map.Entry<String, IntWritable>) (o2)).getValue());
        }
      });

      Map<String, IntWritable> result = new LinkedHashMap<String, IntWritable>();
      for (Iterator<Object> it = list.iterator(); it.hasNext();) {
        Map.Entry<String, IntWritable> entry = (Map.Entry<String, IntWritable>)it.next();
        result.put(entry.getKey(), entry.getValue());
      }
      return result;
    } 

  }

  static void printUsage() {
    System.out.println("termvectorperhost [-r <reduces>] <input> <output>");
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
	Job job = Job.getInstance(conf, "termvectorperhost");
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setMapperClass(MapClass.class);        
    job.setCombinerClass(Combine.class);
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

    FileInputFormat.setInputPaths(job, new Path(other_args.get(0)));
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
    int ret = ToolRunner.run(new TermVectorPerHost(), args);
  
  }
}
