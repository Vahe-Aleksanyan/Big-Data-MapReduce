import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaskD {
   public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "Connectedness Factor");
      job.setJarByClass(TaskD.class);
      MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TaskD.MyPageMapper.class);
      MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TaskD.ConnectednessFactorMapper.class);
      job.setCombinerClass(TaskD.ConnectednessFactorReducer.class);
      job.setReducerClass(TaskD.ConnectednessFactorReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      FileOutputFormat.setOutputPath(job, new Path(args[2]));
      System.exit(job.waitForCompletion(true) ? 0 : 1);
   }

   public static class MyPageMapper extends Mapper<Object, Text, Text, IntWritable> {
      private static final IntWritable zero = new IntWritable(0);
      private Text pageId = new Text();

      protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
         String[] fields = value.toString().split(",", 2);
         if (fields.length >= 1) {
            this.pageId.set(fields[0]);
            context.write(this.pageId, zero);
         }

      }
   }

   public static class ConnectednessFactorMapper extends Mapper<Object, Text, Text, IntWritable> {
      private static final IntWritable one = new IntWritable(1);
      private Text friendId = new Text();

      protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
         String[] fields = value.toString().split(",", 4);
         if (fields.length >= 3) {
            this.friendId.set(fields[2]);
            context.write(this.friendId, one);
         }

      }
   }

   public static class ConnectednessFactorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
      protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
         int sum = 0;

         IntWritable val;
         for(Iterator var5 = values.iterator(); var5.hasNext(); sum += val.get()) {
            val = (IntWritable)var5.next();
         }

         context.write(key, new IntWritable(sum));
      }
   }
}
