import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

import java.util.*;

public class TaskB {


    public static class PageAccessMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final IntWritable one = new IntWritable(1);
        private String fileName;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            fileName = fileSplit.getPath().getName();
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (!"access_logs.csv".equals(fileName)) {
                return;
            }

            String line = value.toString();
            String[] fields = line.split(",");

            if (fields.length >= 3) {
                String pageId = fields[2].trim();

                if (!pageId.isEmpty() && pageId.matches("^[\\x20-\\x7E]*$")) {
                    context.write(new Text(pageId), one);
                }
            }
        }
    }



    public static class PageAccessReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private static final int TOP_N = 10;
        private PriorityQueue<Map.Entry<String, Integer>> topPages =
                new PriorityQueue<>(TOP_N, (a, b) -> a.getValue() - b.getValue());

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            topPages.offer(new AbstractMap.SimpleEntry<>(key.toString(), sum));

            if (topPages.size() > TOP_N) {
                topPages.poll();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            while (!topPages.isEmpty()) {
                Map.Entry<String, Integer> entry = topPages.poll();
                context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        conf.setBoolean("mapreduce.map.speculative", false);
        conf.setBoolean("mapreduce.reduce.speculative", false);

        Job job = Job.getInstance(conf, "Task B - Page Access Count");
        job.setJarByClass(TaskB.class);
        job.setMapperClass(PageAccessMapper.class);
        job.setReducerClass(PageAccessReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
