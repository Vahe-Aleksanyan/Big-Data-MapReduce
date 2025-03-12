package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SingleIterationKMeans {
    public static class KMeansMapper extends Mapper<Object, Text, Text, Text> {
        private List<double[]> centroids = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            Path[] cacheFiles = context.getLocalCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {

                String localPath = cacheFiles[0].toUri().getPath();
                BufferedReader reader = new BufferedReader(new FileReader(localPath));
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.equals("x,y")) {
                        String[] parts = line.split(",");
                        centroids.add(new double[]{Double.parseDouble(parts[0]), Double.parseDouble(parts[1])});
                    }
                }
                reader.close();
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] point = value.toString().split(",");
            if (point[0].equals("x")) return; // Skip header
            double x = Double.parseDouble(point[0]);
            double y = Double.parseDouble(point[1]);

            int closestCentroid = -1;
            double minDistance = Double.MAX_VALUE;

            for (int i = 0; i < centroids.size(); i++) {
                double[] centroid = centroids.get(i);
                double distance = Math.sqrt(Math.pow(x - centroid[0], 2) + Math.pow(y - centroid[1], 2));
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroid = i;
                }
            }

            context.write(new Text(Integer.toString(closestCentroid)), new Text(x + "," + y));
        }
    }

    public static class KMeansReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sumX = 0, sumY = 0;
            int count = 0;

            for (Text value : values) {
                String[] point = value.toString().split(",");
                sumX += Double.parseDouble(point[0]);
                sumY += Double.parseDouble(point[1]);
                count++;
            }

            double newX = sumX / count;
            double newY = sumY / count;

            context.write(key, new Text(newX + "," + newY));
        }
    }

    public static void main(String[] args) throws Exception {

        long startTime = System.currentTimeMillis();

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Single Iteration K-Means");
        job.setJarByClass(SingleIterationKMeans.class);
        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.addCacheFile(new Path(args[2]).toUri()); // Add seed file as distributed cache

        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;
        System.out.println("Execution time: " + executionTime + " ms");

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
