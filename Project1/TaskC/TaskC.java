package com.example;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaskC {
   public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "Facebook Page Count by Country");
      job.setJarByClass(TaskC.class);
      job.setMapperClass(TaskC.FacebookPageCountMapper.class);
      job.setReducerClass(TaskC.FacebookPageCountReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      System.exit(job.waitForCompletion(true) ? 0 : 1);
   }

   public static class FacebookPageCountMapper extends Mapper<Object, Text, Text, IntWritable> {
      private static final IntWritable one = new IntWritable(1);
      private Text country = new Text();

      public void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
         String[] fields = value.toString().trim().split(",");
         if (fields.length >= 4) {
            String countryCode = fields[3].trim();
            if (!countryCode.isEmpty() && countryCode.matches("\\d+")) {
               int code = Integer.parseInt(countryCode);
               if (code >= 1 && code <= 50) {
                  this.country.set(countryCode);
                  context.write(this.country, one);
               }
            }
         }

      }
   }

   public static class FacebookPageCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
      public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
         int count = 0;

         IntWritable val;
         for(Iterator var5 = values.iterator(); var5.hasNext(); count += val.get()) {
            val = (IntWritable)var5.next();
         }

         context.write(key, new IntWritable(count));
      }
   }
}
