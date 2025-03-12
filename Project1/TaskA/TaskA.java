package com.example;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaskA {
   public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      conf.set("mapreduce.input.fileinputformat.split.maxsize", "67108864");
      conf.set("mapreduce.map.output.compress", "true");
      conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
      conf.set("mapreduce.output.fileoutputformat.compress", "true");
      conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
      Job job = Job.getInstance(conf, "Task A - Nationality Filter");
      job.setJarByClass(TaskA.class);
      job.setMapperClass(TaskA.NationalityMapper.class);
      job.setNumReduceTasks(0);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      FileInputFormat.addInputPath(job, new Path(args[1]));
      FileOutputFormat.setOutputPath(job, new Path(args[2]));
      System.exit(job.waitForCompletion(true) ? 0 : 1);
   }

   public static class NationalityMapper extends Mapper<LongWritable, Text, Text, Text> {
      private static final String TARGET_NATIONALITY = "Armenia";
      private Text nameText = new Text();
      private Text hobbyText = new Text();

      public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
         String line = value.toString();
         String[] fields = line.split(",");
         if (!line.startsWith("PersonID") && fields.length >= 5) {
            String name = fields[1];
            String nationality = fields[2];
            String hobby = fields[4];
            if ("Armenia".equals(nationality)) {
               this.nameText.set(name);
               this.hobbyText.set(hobby);
               context.write(this.nameText, this.hobbyText);
            }
         }

      }
   }
}
