import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;


public class PageDetailsJoin {

    public static class TopPagesMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\\s+");
            if (fields.length == 2) {
                String pageId = fields[0].trim();
                String count = fields[1].trim();
                context.write(new Text(pageId), new Text("COUNT:" + count));
            }
        }
    }

    public static class PagesMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length >= 3) {
                String pageId = fields[0].trim();
                String name = fields[1].trim();
                String nationality = fields[2].trim();
                context.write(new Text(pageId), new Text("DETAILS:" + name + "," + nationality));
            }
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String name = "Unknown";
            String nationality = "Unknown";
            Integer count = null;  // Use null to check if COUNT exists

            for (Text val : values) {
                String value = val.toString();
                if (value.startsWith("COUNT:")) {
                    count = Integer.parseInt(value.substring(6));
                } else if (value.startsWith("DETAILS:")) {
                    String[] details = value.substring(8).split(",");
                    if (details.length == 2) {
                        name = details[0];
                        nationality = details[1];
                    }
                }
            }

            // **Only output if the page ID was in the top 10**
            if (count != null) {
                context.write(key, new Text(name + "\t" + nationality ));
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Join Page Details");

        job.setJarByClass(PageDetailsJoin.class);
        job.setReducerClass(JoinReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TopPagesMapper.class);  // Top 10 IDs
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, PagesMapper.class);  // pages.csv

        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
