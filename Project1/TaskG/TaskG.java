import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;

public class TaskG {

    public static class AccessLogMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().trim().split(",");

            if (fields.length >= 5) {
                String byWho = fields[1].trim();
                String accessTimeValue = fields[4].trim();

                if (byWho.matches("\\d+")) {
                    context.write(new Text(byWho), new Text(accessTimeValue));
                }
            }
        }
    }

    public static class AccessLogReducer extends Reducer<Text, Text, Text, Text> {
        private static final long FOURTEEN_DAYS_AGO = System.currentTimeMillis() - 14L * 24 * 60 * 60 * 1000;
        private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long latestAccessTime = 0;

            for (Text value : values) {
                try {
                    long accessTimeMillis = sdf.parse(value.toString()).getTime();
                    latestAccessTime = Math.max(latestAccessTime, accessTimeMillis);
                } catch (Exception ignored) {}
            }

            if (latestAccessTime < FOURTEEN_DAYS_AGO) {
                context.write(key, new Text(""));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Disconnected Users");

        job.setJarByClass(TaskG.class);
        job.setMapperClass(AccessLogMapper.class);
        job.setReducerClass(AccessLogReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
