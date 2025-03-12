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

public class TaskH {
   public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "Popular Facebook Users");
      job.setJarByClass(TaskH.class);
      job.setMapperClass(TaskH.FriendsMapper.class);
      job.setReducerClass(TaskH.FriendsReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      System.exit(job.waitForCompletion(true) ? 0 : 1);
   }

   public static class FriendsMapper extends Mapper<Object, Text, Text, IntWritable> {
      private Text userId = new Text();
      private IntWritable friendCount = new IntWritable();

      protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
         String[] parts = value.toString().split(",");
         if (parts.length > 1) {
            this.userId.set(parts[0]);
            this.friendCount.set(parts.length - 1);
            context.write(this.userId, this.friendCount);
            context.write(new Text("TOTAL_FRIENDSHIPS"), this.friendCount);
            context.write(new Text("TOTAL_USERS"), new IntWritable(1));
         }

      }
   }

   public static class FriendsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
      private int totalFriendships = 0;
      private int totalUsers = 0;
      private double avgFriends = 0.0D;

      protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
         Iterator var4;
         IntWritable val;
         if (key.toString().equals("TOTAL_FRIENDSHIPS")) {
            for(var4 = values.iterator(); var4.hasNext(); this.totalFriendships += val.get()) {
               val = (IntWritable)var4.next();
            }
         } else if (key.toString().equals("TOTAL_USERS")) {
            for(var4 = values.iterator(); var4.hasNext(); this.totalUsers += val.get()) {
               val = (IntWritable)var4.next();
            }
         } else {
            int count = 0;

            IntWritable val;
            for(Iterator var8 = values.iterator(); var8.hasNext(); count += val.get()) {
               val = (IntWritable)var8.next();
            }

            context.write(new Text("USER_" + key.toString()), new IntWritable(count));
         }

      }

      protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
         if (this.totalUsers > 0) {
            this.avgFriends = (double)this.totalFriendships / (double)this.totalUsers;
         }

         context.write(new Text("Average Friendships"), new IntWritable((int)this.avgFriends));
      }
   }
}
