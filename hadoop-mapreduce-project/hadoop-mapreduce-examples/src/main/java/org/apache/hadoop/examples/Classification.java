package org.apache.hadoop.examples;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.kmeans.Cluster;
import org.apache.hadoop.examples.kmeans.Review;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
//import org.mortbay.log.Log;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

@SuppressWarnings("deprecation")
public class Classification {
	
  public static final Log LOG = LogFactory.getLog("org.apache.hadoop.examples.Classification");	

  private final static int maxClusters = 16;

  private enum Counter {
    WORDS, VALUES
  }

  public static Cluster[] centroids = new Cluster[maxClusters];
  public static Cluster[] centroids_ref = new Cluster[maxClusters];

  public static String strModelFile = "/localhome/hadoop1/work/kmeans/mr_centroids";
  // Input data should have the following format. Each line of input record represents one movie and all of its reviews. 
  // Each record has the format: <movie_id><:><reviewer><_><rating><,><movie_id><:><reviewer><_><rating><,> .....

  public static class MapClass extends
  Mapper<LongWritable, Text, IntWritable, Text> {
    private int totalClusters;


    public void configure(JobConf conf){
      try {
        totalClusters = initializeCentroids();
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      }
    }

    public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {
      String movieIdStr = new String();
      String reviewStr = new String();
      String userIdStr = new String();
      String reviews = new String();
      String line = new String();
      String tok = new String("");
      long movieId;
      int review, userId, p,q,r,rater,rating,movieIndex;
      int clusterId = 0;
      int[] n = new int[maxClusters];
      float[] sq_a = new float[maxClusters];
      float[] sq_b = new float[maxClusters];
      float[] numer = new float[maxClusters];
      float[] denom = new float[maxClusters];
      float max_similarity = 0.0f;
      float similarity = 0.0f;
      Cluster movie = new Cluster();

      line = ((Text) value).toString();
      movieIndex = line.indexOf(":");
      for (r = 0; r < maxClusters; r++) {
        numer[r] = 0.0f;
        denom[r] = 0.0f;
        sq_a[r] = 0.0f;
        sq_b[r] = 0.0f;
        n[r] = 0;
      }
      if (movieIndex > 0) {
        movieIdStr = line.substring(0, movieIndex);
        movieId = Long.parseLong(movieIdStr);
        movie.movie_id = movieId;
        reviews = line.substring(movieIndex + 1);
        StringTokenizer token = new StringTokenizer(reviews, ",");

        while (token.hasMoreTokens()) {
          tok = token.nextToken();
          int reviewIndex = tok.indexOf("_");
          userIdStr = tok.substring(0, reviewIndex);
          reviewStr = tok.substring(reviewIndex + 1);
          userId = Integer.parseInt(userIdStr);
          review = Integer.parseInt(reviewStr);
          for (r = 0; r < totalClusters; r++) {
            for (q = 0; q < centroids_ref[r].total; q++) {
              rater = centroids_ref[r].reviews.get(q).rater_id;
              rating = (int) centroids_ref[r].reviews.get(q).rating;
              if (userId == rater) {
                numer[r] += (float) (review * rating);
                sq_a[r] += (float) (review * review);
                sq_b[r] += (float) (rating * rating);
                n[r]++; // counter 
                break; // to avoid multiple ratings by the same reviewer
              }
            }
          }
        }
        for (p = 0; p < totalClusters; p++) {
          denom[p] = (float) ((Math.sqrt((double) sq_a[p])) * (Math
              .sqrt((double) sq_b[p])));
          if (denom[p] > 0) {
            similarity = numer[p] / denom[p];
            if (similarity > max_similarity) {
              max_similarity = similarity;
              clusterId = p;
            }
          }
        }
        context.write(new IntWritable(clusterId), new Text(movieIdStr));
      }
    }
    public void close() {
    }
  }


  public static class Reduce 
  extends Reducer<IntWritable, Text, IntWritable, Text> {
    @Override
    public void reduce(IntWritable key, Iterable<Text> values,Context context)
    throws IOException, InterruptedException {

      for (Text value: values) {
        Text cr = (Text) value;
        context.write(key, cr);
        
      }
    }
  }

  static void printUsage() {
    System.out
    .println("classification [-r <reduces>] <input> <output>");
    System.exit(1);
  }

  public static int initializeCentroids() throws FileNotFoundException {
    int i, k, index, numClust = 0;
    Review rv;
    String reviews = new String("");
    String SingleRv = new String("");
    File modelFile;
    Scanner opnScanner;
    for (i = 0; i < maxClusters; i++) {
      centroids[i] = new Cluster();
      centroids_ref[i] = new Cluster();
    }
    modelFile = new File(strModelFile);
    opnScanner = new Scanner(modelFile);
    while(opnScanner.hasNext()){
      k = opnScanner.nextInt();
      centroids_ref[k].similarity = opnScanner.nextFloat();
      centroids_ref[k].movie_id = opnScanner.nextLong();
      centroids_ref[k].total = opnScanner.nextShort();
      reviews = opnScanner.next();
      Scanner revScanner = new Scanner(reviews).useDelimiter(",");
      while(revScanner.hasNext()){
        SingleRv = revScanner.next();
        index = SingleRv.indexOf("_");
        String reviewer = new String(SingleRv.substring(0,index));
        String rating = new String(SingleRv.substring(index+1));
        rv = new Review();
        rv.rater_id = Integer.parseInt(reviewer);
        rv.rating = (byte) Integer.parseInt(rating);
        centroids_ref[k].reviews.add(rv);
      }
    }
    // implementing naive bubble sort as maxClusters is small
    // sorting is done to assign top most cluster ids in each iteration
    for( int pass = 1; pass < maxClusters; pass++) {
      for (int u = 0; u < maxClusters - pass; u++) {
        if (centroids_ref[u].movie_id < centroids_ref[u+1].movie_id ) {
          Cluster temp = new Cluster(centroids_ref[u]);
          centroids_ref[u] = centroids_ref[u+1];
          centroids_ref[u+1] = temp;
        }
      }
    }
    for(int l=0; l< maxClusters; l++) {
      if(centroids_ref[l].movie_id != -1){
        numClust++;
      }
    }
    return numClust;
  }

  public static int main(String[] args) throws Exception {

    int i;
    String outPath;
    int numMaps = 0, numReds = 0;

    List<String> other_args = new ArrayList<String>();
    for (i = 0; i < args.length; ++i) {
      try {
        if ("-m".equals(args[i])) {
          numMaps = Integer.parseInt(args[++i]);
        } else if ("-r".equals(args[i])) {
          numReds = Integer.parseInt(args[++i]);
        } else {
          other_args.add(args[i]);
        }
      } catch (NumberFormatException except) {
        System.out.println("ERROR: Integer expected instead of "
            + args[i]);
        printUsage();
      } catch (ArrayIndexOutOfBoundsException except) {
        System.out.println("ERROR: Required parameter missing from "
            + args[i - 1]);
        printUsage(); // exits
      }
    }
    // Make sure there are exactly 2 parameters left.
    if (other_args.size() != 2) {
      System.out.println("ERROR: Wrong number of parameters: "
          + other_args.size() + " instead of 2.");
      printUsage();
    }

    Date startTime = new Date();
    System.out.println("Job started: " + startTime);
    Date startIteration;
    Date endIteration;
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "classifacation");
    job.setJobName("classification");
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setMapperClass(MapClass.class);
    job.setReducerClass(Reduce.class);
    job.setNumReduceTasks(numReds);
    FileInputFormat.setInputPaths(job, new Path(other_args.get(0)));
    outPath = new String(other_args.get(1));
    FileOutputFormat.setOutputPath(job, new Path(outPath));
    startIteration = new Date();
    Boolean waitforCompletion = job.waitForCompletion(true) ;
    endIteration = new Date();
    System.out.println("The iteration took "
        + (endIteration.getTime() - startIteration.getTime()) / 1000
        + " seconds.");
    System.exit(waitforCompletion ? 0 : 1);
    return 0;
  }
}
