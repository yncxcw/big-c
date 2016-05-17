/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }
  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  static void printUsage(){
	  System.out.println("Usage: wordcount [-r <reduces>]<in> [<in>...] <out>");
  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
   
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
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
	return;
  }
}
