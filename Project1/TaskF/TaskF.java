package com.example;

import java.io.IOException;
import java.util.HashSet;
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

public class TaskF {
   public static void main(String[] args) throws Exception {
      if (args.length < 3) {
         System.err.println("Usage: com.example.TaskF <Friends input path> <AccessLog input path> <MyPage input path> <output path>");
         System.exit(-1);
      }

      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "Friends Never Accessed");
      job.setJarByClass(TaskF.class);
      MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TaskF.FriendshipMapper.class);
      MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TaskF.AccessLogMapper.class);
      job.setReducerClass(TaskF.FriendNotAccessedReducer.class);
      job.setOutputKeyClass(IntWritable.class);
      job.setOutputValueClass(Text.class);
      FileOutputFormat.setOutputPath(job, new Path(args[2]));
      System.exit(job.waitForCompletion(true) ? 0 : 1);
   }

   public static class FriendshipMapper extends Mapper<Object, Text, IntWritable, Text> {
      private final IntWritable personID = new IntWritable();
      private final Text relation = new Text();

      public void map(Object key, Text value, Mapper<Object, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
         String line = value.toString().trim();
         if (!line.startsWith("PersonID")) {
            String[] fields = line.split(",");
            if (fields.length >= 3) {
               try {
                  int p1 = Integer.parseInt(fields[1].trim());
                  int p2 = Integer.parseInt(fields[2].trim());
                  this.personID.set(p1);
                  this.relation.set("FRIEND:" + p2);
                  context.write(this.personID, this.relation);
               } catch (NumberFormatException var8) {
                  return;
               }
            }

         }
      }
   }

   public static class AccessLogMapper extends Mapper<Object, Text, IntWritable, Text> {
      private final IntWritable personID = new IntWritable();
      private final Text relation = new Text();

      public void map(Object key, Text value, Mapper<Object, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
         String line = value.toString().trim();
         if (!line.startsWith("PersonID") && !line.startsWith("ByWho")) {
            String[] fields = line.split(",");
            if (fields.length >= 3) {
               try {
                  int p1 = Integer.parseInt(fields[1].trim());
                  int p2 = Integer.parseInt(fields[2].trim());
                  this.personID.set(p1);
                  this.relation.set("ACCESSED:" + p2);
                  context.write(this.personID, this.relation);
               } catch (NumberFormatException var8) {
                  return;
               }
            }

         }
      }
   }

   public static class FriendNotAccessedReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
      public void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
         HashSet<Integer> friends = new HashSet();
         HashSet<Integer> accessedPages = new HashSet();
         Iterator var6 = values.iterator();

         while(var6.hasNext()) {
            Text value = (Text)var6.next();
            String[] parts = value.toString().split(":");
            if (parts[0].equals("FRIEND")) {
               friends.add(Integer.parseInt(parts[1]));
            } else if (parts[0].equals("ACCESSED")) {
               accessedPages.add(Integer.parseInt(parts[1]));
            }
         }

         var6 = friends.iterator();

         while(var6.hasNext()) {
            Integer friend = (Integer)var6.next();
            if (!accessedPages.contains(friend)) {
               context.write(key, new Text("NEVER ACCESSED FRIEND " + friend));
            }
         }

      }
   }
}
