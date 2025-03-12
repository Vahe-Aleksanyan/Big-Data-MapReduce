//package org.example;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.*;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import java.io.*;
//import java.net.URI;
//import java.util.ArrayList;
//import java.util.List;
//public class EarlyStoppingKMeans {
//    private static final double THRESHOLD = 0.001;  // Convergence threshold
//
//    public static class KMeansMapper extends Mapper<Object, Text, Text, Text> {
//        private List<double[]> centroids = new ArrayList<>();
//
//        @Override
//        protected void setup(Context context) throws IOException {
//            URI[] cacheFiles = context.getCacheFiles();
//
//            if (cacheFiles != null && cacheFiles.length > 0) {
//                BufferedReader reader = new BufferedReader(new FileReader("centroids.csv"));
//                String line;
//                while ((line = reader.readLine()) != null) {
//                    String[] parts = line.split("\\s+|,");
//                    centroids.add(new double[]{Double.parseDouble(parts[1]), Double.parseDouble(parts[2])});
//                }
//                reader.close();
//            }
//        }
//
//        @Override
//        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//            String[] parts = value.toString().split(",");
//            double x = Double.parseDouble(parts[0]);
//            double y = Double.parseDouble(parts[1]);
//
//            int closestCentroid = -1;
//            double minDistance = Double.MAX_VALUE;
//
//            for (int i = 0; i < centroids.size(); i++) {
//                double[] centroid = centroids.get(i);
//                double distance = Math.sqrt(Math.pow(x - centroid[0], 2) + Math.pow(y - centroid[1], 2));
//                if (distance < minDistance) {
//                    minDistance = distance;
//                    closestCentroid = i;
//                }
//            }
//
//            context.write(new Text(Integer.toString(closestCentroid)), new Text(x + "," + y));
//        }
//    }
//
//    public static class KMeansReducer extends Reducer<Text, Text, Text, Text> {
//        @Override
//        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            double sumX = 0, sumY = 0;
//            int count = 0;
//
//            for (Text value : values) {
//                String[] point = value.toString().split(",");
//                sumX += Double.parseDouble(point[0]);
//                sumY += Double.parseDouble(point[1]);
//                count++;
//            }
//
//            if (count > 0) {
//                double newX = sumX / count;
//                double newY = sumY / count;
//                context.write(key, new Text(newX + "," + newY));
//            }
//        }
//    }
//
//    public static boolean hasConverged(FileSystem fs, Path oldCentroids, Path newCentroids) throws IOException {
//        if (!fs.exists(oldCentroids) || !fs.exists(newCentroids)) return false;
//
//        BufferedReader oldReader = new BufferedReader(new InputStreamReader(fs.open(oldCentroids)));
//        BufferedReader newReader = new BufferedReader(new InputStreamReader(fs.open(newCentroids)));
//
//        String oldLine, newLine;
//        while ((oldLine = oldReader.readLine()) != null && (newLine = newReader.readLine()) != null) {
//            String[] oldParts = oldLine.split("\\s+|,");
//            String[] newParts = newLine.split("\\s+|,");
//
//            double oldX = Double.parseDouble(oldParts[1]);
//            double oldY = Double.parseDouble(oldParts[2]);
//            double newX = Double.parseDouble(newParts[1]);
//            double newY = Double.parseDouble(newParts[2]);
//
//            double distance = Math.sqrt(Math.pow(oldX - newX, 2) + Math.pow(oldY - newY, 2));
//            if (distance > THRESHOLD) {
//                oldReader.close();
//                newReader.close();
//                return false;
//            }
//        }
//
//        oldReader.close();
//        newReader.close();
//        return true;
//    }
//
//    public static void main(String[] args) throws Exception {
//        if (args.length < 4) {
//            System.err.println("Usage: EarlyStoppingKMeans <input> <output> <initial centroids> <iterations>");
//            System.exit(1);
//        }
//
//        int R = Integer.parseInt(args[3]);
//        Configuration conf = new Configuration();
//        FileSystem fs = FileSystem.get(conf);
//        Path inputPath = new Path(args[0]);
//        Path outputPath = new Path(args[1]);
//        Path centroidsPath = new Path(args[2]);
//
//        if (fs.exists(outputPath)) fs.delete(outputPath, true);
//        Path currentCentroids = new Path(outputPath, "centroids.csv");
//        FileUtil.copy(fs, centroidsPath, fs, currentCentroids, false, conf);
//
//        for (int i = 0; i < R; i++) {
//            Job job = Job.getInstance(conf, "KMeans Iteration " + i);
//            job.setJarByClass(EarlyStoppingKMeans.class);
//            job.setMapperClass(KMeansMapper.class);
//            job.setReducerClass(KMeansReducer.class);
//            job.setMapOutputKeyClass(Text.class);
//            job.setMapOutputValueClass(Text.class);
//            job.setOutputKeyClass(Text.class);
//            job.setOutputValueClass(Text.class);
//
//            FileInputFormat.addInputPath(job, inputPath);
//            Path iterationOutput = new Path(outputPath, "iteration_" + i);
//            FileOutputFormat.setOutputPath(job, iterationOutput);
//            job.addCacheFile(new URI(currentCentroids.toString() + "#centroids.csv"));
//
//            if (!job.waitForCompletion(true)) {
//                System.exit(1);
//            }
//
//            Path newCentroids = new Path(iterationOutput, "part-r-00000");
//            if (!fs.exists(newCentroids)) {
//                System.exit(1);
//            }
//
//            if (hasConverged(fs, currentCentroids, newCentroids)) {
//                System.out.println("KMeans converged at iteration " + i);
//                break;
//            }
//
//            fs.delete(currentCentroids, false);
//            fs.rename(newCentroids, currentCentroids);
//        }
//    }
//}
//





package org.example;

        import org.apache.hadoop.conf.Configuration;
        import org.apache.hadoop.fs.*;
        import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapreduce.Job;
        import org.apache.hadoop.mapreduce.Mapper;
        import org.apache.hadoop.mapreduce.Reducer;
        import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
        import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

        import java.io.*;
        import java.net.URI;
        import java.util.ArrayList;
        import java.util.List;

public class EarlyStoppingKMeans {
    private static final double THRESHOLD = 0.001;

    public static class KMeansMapper extends Mapper<Object, Text, Text, Text> {
        private List<double[]> centroids = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                BufferedReader reader = new BufferedReader(new FileReader("centroids.csv"));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\t");  // Use tab instead of comma
                    if (parts.length == 2) {  // Ensure valid format
                        String[] coords = parts[1].split(",");
                        centroids.add(new double[]{Double.parseDouble(coords[0]), Double.parseDouble(coords[1])});
                    }
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

            if (count > 0) {
                double newX = sumX / count;
                double newY = sumY / count;
                context.write(key, new Text(newX + "," + newY));
            }
        }
    }

    public static boolean hasConverged(FileSystem fs, Path oldCentroids, Path newCentroids) throws IOException {
        if (!fs.exists(oldCentroids) || !fs.exists(newCentroids)) return false;

        BufferedReader oldReader = new BufferedReader(new InputStreamReader(fs.open(oldCentroids)));
        BufferedReader newReader = new BufferedReader(new InputStreamReader(fs.open(newCentroids)));

        String oldLine, newLine;
        while ((oldLine = oldReader.readLine()) != null && (newLine = newReader.readLine()) != null) {
            String[] oldParts = oldLine.split("\\s+|,");
            String[] newParts = newLine.split("\\s+|,");

            double oldX, oldY, newX, newY;
            if (oldParts.length == 2) { // Format: x,y (no id)
                oldX = Double.parseDouble(oldParts[0]);
                oldY = Double.parseDouble(oldParts[1]);
            } else if (oldParts.length >= 3) { // Format: id \t x,y
                oldX = Double.parseDouble(oldParts[1]);
                oldY = Double.parseDouble(oldParts[2]);
            } else {
                throw new IOException("Invalid format in old centroids file");
            }

            if (newParts.length == 2) {
                newX = Double.parseDouble(newParts[0]);
                newY = Double.parseDouble(newParts[1]);
            } else if (newParts.length >= 3) {
                newX = Double.parseDouble(newParts[1]);
                newY = Double.parseDouble(newParts[2]);
            } else {
                throw new IOException("Invalid format in new centroids file");
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


    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: MultiIterationKMeans <input> <output> <initial centroids> <iterations>");
            System.exit(1);
        }
        long startTime = System.currentTimeMillis();
        int R = Integer.parseInt(args[3]);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        Path centroidsPath = new Path(args[2]);

        if (fs.exists(outputPath)) fs.delete(outputPath, true);
        Path currentCentroids = new Path(outputPath, "centroids.csv");
        FileUtil.copy(fs, centroidsPath, fs, currentCentroids, false, conf);

        for (int i = 0; i < R; i++) {
            Job job = Job.getInstance(conf, "KMeans Iteration " + i);
            job.setJarByClass(EarlyStoppingKMeans.class);
            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, inputPath);
            Path iterationOutput = new Path(outputPath, "iteration_" + i);
            FileOutputFormat.setOutputPath(job, iterationOutput);
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
