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

public class Task2 {

  public static class LineSumMapper extends Mapper<Object, Text, NullWritable, IntWritable>{
    private final static IntWritable lineTotal = new IntWritable();
    private final static NullWritable nw = NullWritable.get();
      
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String val = value.toString();

      int count = 0;
      for (int i = val.indexOf(',') + 1; i < val.length(); i++) {
        if (val.charAt(i) != ',') {
          count++;
        }
      }
      lineTotal.set(count);
      context.write(nw, lineTotal);
    }
  }

  public static class IntSumReducer extends Reducer<NullWritable,IntWritable,NullWritable,IntWritable> {
    private final static IntWritable result = new IntWritable();
    private final static NullWritable nw = NullWritable.get();

    public void reduce(NullWritable nw, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(nw, result);
    }
  }
    
    
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", "");

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }

    Job job = Job.getInstance(conf, "Task2");
    job.setJarByClass(Task2.class);
    
    job.setMapperClass(LineSumMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setNumReduceTasks(1);

    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(IntWritable.class);
    
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    
    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
