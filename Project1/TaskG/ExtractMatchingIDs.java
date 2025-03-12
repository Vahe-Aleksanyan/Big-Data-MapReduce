import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

public class ExtractMatchingIDs {

    public static class IDFilterMapper extends Mapper<Object, Text, Text, Text> {
        private final Set<String> validIDs = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            Path filterFilePath = new Path(conf.get("filterFilePath")); // Get path from job config
            FileSystem fs = FileSystem.get(conf);

            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(filterFilePath)))) {
                String line;
                while ((line = br.readLine()) != null) {
                    validIDs.add(line.trim()); // Store valid IDs from the newly created file
                }
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length >= 2) {
                String personID = fields[0].trim();
                String name = fields[1].trim();

                if (validIDs.contains(personID)) { // Check if ID is in the set
                    context.write(new Text(personID), new Text(name));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("filterFilePath", args[1]); // Path to the new file containing filtered IDs

        Job job = Job.getInstance(conf, "Extract Matching IDs");
        job.setJarByClass(ExtractMatchingIDs.class);
        job.setMapperClass(IDFilterMapper.class);
        job.setNumReduceTasks(0); // No reducer needed

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[2])); // Path to pages.csv
        FileOutputFormat.setOutputPath(job, new Path(args[3])); // Output directory

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

