import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;

public class TopYelpPaloAlto {
    public static class filterMapper extends Mapper<Object, Text, Text, Text>{

        private static BufferedReader br;
        private static Text rating = new Text();
        private static Text business_id = new Text();

        private static HashMap<String, String> businessDetails_HM = new HashMap<>();

        protected void setup(Context context) throws IOException, InterruptedException {

            Path[] cachefiles = context.getLocalCacheFiles();

            if (cachefiles!=null)
            {
                for (Path ph:cachefiles) {
                    searchCacheFile(ph);
                }
            }
        }

        private void searchCacheFile(Path ph) throws IOException {
            String line;

            br = new BufferedReader(new FileReader(ph.getName().toString()));
            while ((line = br.readLine()) != null){
                String[] rec = line.split("::");
                if (rec[1].toLowerCase().contains("palo alto")){
                    businessDetails_HM.put(rec[0].trim(), rec[1].trim());
                }
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] ratingData = value.toString().trim().split("::");

            if(businessDetails_HM.containsKey(ratingData[2].trim()))
            {
                business_id.set(ratingData[2].trim());
                rating.set(ratingData[3]);
                context.write(business_id, rating);
            }
        }
    }

    public static class AverageReducer extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> ratings, Context context) throws IOException, InterruptedException {
            float count =0;
            float sum = 0;
            for (Text r:ratings) {
                sum += Float.parseFloat(r.toString());
                count++;
            }

            float avg = (sum / count);
            String avgString = String.valueOf(avg);
            context.write(new Text(key), new Text(avgString));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "TopYelpPaloAlto");
        DistributedCache.addCacheFile(new Path(args[0]).toUri(),job.getConfiguration());
        job.setJarByClass(TopYelpPaloAlto.class);
        job.setMapperClass(filterMapper.class);
        job.setCombinerClass(AverageReducer.class);
        job.setReducerClass(AverageReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}