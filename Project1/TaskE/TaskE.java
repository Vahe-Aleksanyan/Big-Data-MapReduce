import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaskE {
   public static void main(String[] args) throws Exception {
      if (args.length != 2) {
         System.err.println("Usage: TaskE <AccessLog input path> <output path>");
         System.exit(-1);
      }

      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "Favorites Detection");
      job.setJarByClass(TaskE.class);
      job.setMapperClass(TaskE.AccessLogMapper.class);
      job.setCombinerClass(TaskE.FavoritesReducer.class);
      job.setReducerClass(TaskE.FavoritesReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      System.exit(job.waitForCompletion(true) ? 0 : 1);
   }

   public static class AccessLogMapper extends Mapper<Object, Text, Text, Text> {
      private Text userId = new Text();
      private Text pageId = new Text();

      protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
         String[] fields = value.toString().split(",");
         if (fields.length >= 3) {
            this.userId.set(fields[1]);
            this.pageId.set(fields[2]);
            context.write(this.userId, this.pageId);
         }

      }
   }

   public static class FavoritesReducer extends Reducer<Text, Text, Text, Text> {
      private Text result = new Text();

      protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
         int totalAccesses = 0;
         Set<String> distinctPages = new HashSet();
         Iterator var6 = values.iterator();

         while(var6.hasNext()) {
            Text value = (Text)var6.next();
            ++totalAccesses;
            distinctPages.add(value.toString());
         }

         this.result.set(totalAccesses + "," + distinctPages.size());
         context.write(key, this.result);
      }
   }
}
