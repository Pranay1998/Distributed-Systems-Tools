import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task3 {

  public static class UserReviewMapper extends Mapper<Object, Text, IntWritable, IntWritable>{
    private final static IntWritable user = new IntWritable();
    private final static IntWritable rating = new IntWritable();
    private final static NullWritable nw = NullWritable.get();
      
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String val = value.toString();

      int user_index = 1;
      boolean prev = false;
      for (int i = val.indexOf(',') + 1; i < val.length(); i++) {
        char c = val.charAt(i);
        boolean comma = (c == ',');

        if (!comma || prev) {
          user.set(user_index);
          rating.set(comma ? 0 : 1);
          context.write(user, rating);
        }

        if (comma) {
          user_index++;
          prev = true;
        }
      }
    }
  }

  public static class ReviewSumReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
    private final static IntWritable result = new IntWritable();
    private final static NullWritable nw = NullWritable.get();

    public void reduce(IntWritable user, Iterable<IntWritable> ratings, Context context) throws IOException, InterruptedException {
      //At what length is multithreading worth it?
      int sum = 0;
      for (IntWritable rating : ratings) {
        sum += rating.get();
      }
      result.set(sum);
      context.write(user, result);
    }
  }
    
    
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }

    Job job = Job.getInstance(conf, "Task3");
    job.setJarByClass(Task3.class);
    
    job.setMapperClass(UserReviewMapper.class);
    job.setReducerClass(ReviewSumReducer.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(IntWritable.class);
    
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    
    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
