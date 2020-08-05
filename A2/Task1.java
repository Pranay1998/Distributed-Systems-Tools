import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task1 {

  public static class MaxUserMapper extends Mapper<Object, Text, Text, Text>{
    private Text movie = new Text();
    private Text highestUsers = new Text();
      
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String val = value.toString();

      int user_index = 1;
      int i = val.indexOf(',');
      movie.set(val.substring(0, i));
      i++;

      StringBuilder sb = new StringBuilder();
      char max = '0';
      while (i < val.length()) {
        char c = val.charAt(i);
        if (c != ',') {
          if (c > max) {
            max = c;
            sb.setLength(0);
            sb.append(user_index);
          }
          else if (c == max) {
            sb.append(',');
            sb.append(user_index);
          }
        } else {
          user_index++;
        }
        i++;
      }

      highestUsers.set(sb.toString());
      context.write(movie, highestUsers);
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

    Job job = Job.getInstance(conf, "Task1");
    job.setJarByClass(Task1.class);
    
    job.setMapperClass(MaxUserMapper.class);
    job.setNumReduceTasks(0);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
