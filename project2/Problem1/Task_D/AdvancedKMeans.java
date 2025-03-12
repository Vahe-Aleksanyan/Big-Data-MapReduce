package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

// Custom Writable to hold the sum of coordinates and count of points.
public class AdvancedKMeans {
    private static final double THRESHOLD = 0.001;

    public static class PointSumWritable implements Writable {
        private double sumX;
        private double sumY;
        private int count;


        public PointSumWritable() {}

        public PointSumWritable(double sumX, double sumY, int count) {
            this.sumX = sumX;
            this.sumY = sumY;
            this.count = count;
        }

        public double getSumX() {
            return sumX;
        }

        public double getSumY() {
            return sumY;
        }

        public int getCount() {
            return count;
        }

        public void set(double sumX, double sumY, int count) {
            this.sumX = sumX;
            this.sumY = sumY;
            this.count = count;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeDouble(sumX);
            out.writeDouble(sumY);
            out.writeInt(count);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            sumX = in.readDouble();
            sumY = in.readDouble();
            count = in.readInt();
        }

        @Override
        public String toString() {
            // Return the average (centroid) when printing.
            return (count == 0 ? "0,0" : (sumX / count) + "," + (sumY / count));
        }
    }

    // Mapper: assigns each data point to its closest centroid.
    public static class KMeansMapper extends Mapper<Object, Text, Text, PointSumWritable> {
        private List<double[]> centroids = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                BufferedReader reader = new BufferedReader(new FileReader("centroids.csv"));
                String line;
                while ((line = reader.readLine()) != null) {
                    // Accept two formats: "id<TAB>x,y" or simply "x,y"
                    String[] parts = line.split("\t");
                    double x, y;
                    if (parts.length == 2) {
                        String[] coords = parts[1].split(",");
                        x = Double.parseDouble(coords[0]);
                        y = Double.parseDouble(coords[1]);
                    } else {
                        String[] coords = line.split(",");
                        x = Double.parseDouble(coords[0]);
                        y = Double.parseDouble(coords[1]);
                    }
                    centroids.add(new double[]{x, y});
                }
                reader.close();
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            double x = Double.parseDouble(parts[0]);
            double y = Double.parseDouble(parts[1]);

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
            // Emit the centroid id as key and the point (with count 1) as value.
            context.write(new Text(Integer.toString(closestCentroid)), new PointSumWritable(x, y, 1));
        }
    }

    // Combiner: aggregates partial sums and counts.
    public static class KMeansCombiner extends Reducer<Text, PointSumWritable, Text, PointSumWritable> {
        @Override
        public void reduce(Text key, Iterable<PointSumWritable> values, Context context)
                throws IOException, InterruptedException {
            double sumX = 0, sumY = 0;
            int count = 0;
            for (PointSumWritable val : values) {
                sumX += val.getSumX();
                sumY += val.getSumY();
                count += val.getCount();
            }
            context.write(key, new PointSumWritable(sumX, sumY, count));
        }
    }

    // Reducer: computes the new centroid as the average of points assigned.
    public static class KMeansReducer extends Reducer<Text, PointSumWritable, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<PointSumWritable> values, Context context)
                throws IOException, InterruptedException {
            double sumX = 0, sumY = 0;
            int count = 0;
            for (PointSumWritable val : values) {
                sumX += val.getSumX();
                sumY += val.getSumY();
                count += val.getCount();
            }
            if (count > 0) {
                double newX = sumX / count;
                double newY = sumY / count;
                context.write(key, new Text(newX + "," + newY));
            }
        }
    }

    // The same convergence check method from your previous code.
    public static boolean hasConverged(FileSystem fs, Path oldCentroids, Path newCentroids) throws IOException {
        if (!fs.exists(oldCentroids) || !fs.exists(newCentroids)) return false;

        BufferedReader oldReader = new BufferedReader(new InputStreamReader(fs.open(oldCentroids)));
        BufferedReader newReader = new BufferedReader(new InputStreamReader(fs.open(newCentroids)));

        String oldLine, newLine;
        while ((oldLine = oldReader.readLine()) != null && (newLine = newReader.readLine()) != null) {
            double oldX, oldY, newX, newY;
            String[] oldParts = oldLine.split("\\s+|,");
            String[] newParts = newLine.split("\\s+|,");

            // Process old centroids line
            if (oldParts.length == 2) {
                oldX = Double.parseDouble(oldParts[0]);
                oldY = Double.parseDouble(oldParts[1]);
            } else if (oldParts.length >= 3) {
                oldX = Double.parseDouble(oldParts[1]);
                oldY = Double.parseDouble(oldParts[2]);
            } else {
                oldReader.close();
                newReader.close();
                throw new IOException("Invalid format in old centroids file: " + oldLine);
            }

            // Process new centroids line
            if (newParts.length == 2) {
                newX = Double.parseDouble(newParts[0]);
                newY = Double.parseDouble(newParts[1]);
            } else if (newParts.length >= 3) {
                newX = Double.parseDouble(newParts[1]);
                newY = Double.parseDouble(newParts[2]);
            } else {
                oldReader.close();
                newReader.close();
                throw new IOException("Invalid format in new centroids file: " + newLine);
            }

            double distance = Math.sqrt(Math.pow(oldX - newX, 2) + Math.pow(oldY - newY, 2));
            if (distance > THRESHOLD) {
                oldReader.close();
                newReader.close();
                return false;
            }
        }

        oldReader.close();
        newReader.close();
        return true;
    }


    // Main driver that runs multiple iterations with early stopping.
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: AdvancedKMeans <input> <output> <initial centroids> <iterations>");
            System.exit(1);
        }

        long startTime = System.currentTimeMillis();
        int iterations = Integer.parseInt(args[3]);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        Path centroidsPath = new Path(args[2]);

        if (fs.exists(outputPath)) fs.delete(outputPath, true);
        Path currentCentroids = new Path(outputPath, "centroids.csv");
        FileUtil.copy(fs, centroidsPath, fs, currentCentroids, false, conf);

        for (int i = 0; i < iterations; i++) {
            Job job = Job.getInstance(conf, "Advanced KMeans Iteration " + i);
            job.setJarByClass(AdvancedKMeans.class);
            job.setMapperClass(KMeansMapper.class);
            job.setCombinerClass(KMeansCombiner.class);
            job.setReducerClass(KMeansReducer.class);

            // Set the output types for the map phase.
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(PointSumWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, inputPath);
            Path iterationOutput = new Path(outputPath, "iteration_" + i);
            FileOutputFormat.setOutputPath(job, iterationOutput);

            // Use DistributedCache for the current centroids.
            job.addCacheFile(new URI(currentCentroids.toString() + "#centroids.csv"));

            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }

            Path newCentroids = new Path(iterationOutput, "part-r-00000");
            if (!fs.exists(newCentroids)) {
                System.exit(1);
            }

            System.out.println("Starting iteration " + i);
            if (hasConverged(fs, currentCentroids, newCentroids)) {
                System.out.println("KMeans converged at iteration " + i);
                break;
            }

            fs.delete(currentCentroids, false);
            fs.rename(newCentroids, currentCentroids);
        }


        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;
        System.out.println("Execution time: " + executionTime + " ms");
    }
}
