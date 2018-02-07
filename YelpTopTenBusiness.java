import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class YelpTopTenBusiness {

    public static class ReviewsMapper extends Mapper<LongWritable, Text, Text, Text> {
        protected void map(LongWritable lineno, Text record, Context context) throws IOException, InterruptedException {
            // get required fields from reviews.csv
            String[] rec = record.toString().split("::");
            String business_id = rec[2];
            String rating = rec[3];
            context.write(new Text(business_id), new Text("reviews\t" + rating));
        }
    }

    public static class BusinessDetailsMapper extends Mapper<LongWritable, Text, Text, Text> {
        protected void map(LongWritable lineno, Text record, Context context) throws IOException, InterruptedException {
            // get required fields from business.csv
            String[] rec = record.toString().split("::");
            String business_id = rec[0];
            String address = rec[1];
            String categories = rec[2];
            context.write(new Text(business_id), new Text("details\t"+ address +"::"+categories));
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text,Text> {
        protected void reduce(Text key, Iterable<Text> records, Context context) throws IOException, InterruptedException {
            // calculate average by reducing sum and count values
            float sum = 0;
            float count = 0;

            Text address = new Text();
            Text categories = new Text();
            Text business_id = key;

            for (Text rec : records) {
                String[] recSplit = rec.toString().split("\t");
                if(recSplit[0].equals("reviews")){
                    // if the line came from Ratings mapper
                    sum += Float.parseFloat(recSplit[1]);
                    count++;
                } else if(recSplit[0].equals("details")){
                    // if the line came from business details mapper
                    String[] details = recSplit[1].split("::");
                    address.set(details[0]);
                    categories.set(details[1]);
                }
            }

            float avg = (sum / count);
            FloatWritable average = new FloatWritable();
            average.set(avg);
            Text joinRes = new Text();
            joinRes.set(address + "\t" + categories + "\t" + business_id +"::" + average);
            context.write(business_id, joinRes);
        }
    }

    // use rating as a key to sort
    public static class SortingMapper extends Mapper<LongWritable, Text, FloatWritable, Text> {
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String combinedRecord[] = value.toString().split("::");     // separates details and average
            String[] details = combinedRecord[0].split("\t");   // separates each of the fields in details
            String address = details[1];
            String categories = details[2];
            String biz_id = details[3];
            String average = combinedRecord[1];

            FloatWritable ratingKey = new FloatWritable();
            ratingKey.set(Float.parseFloat(average));

            String allDetails = address + "::" + categories + "::" + biz_id + "::" + average;
            context.write(ratingKey, new Text(allDetails));
        }
    }

    // emit top 10 businesses by rating
    public static class TopTenBusinessReducer extends Reducer<FloatWritable, Text, Text, Text> {
        private int count = 0;

        protected void reduce(FloatWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text v : values) {
                String[] fields = v.toString().split("::");
                String addr = fields[0].toString();
                String categories = fields[1].toString();
                String biz_id = fields[2].toString();
                String avg = fields[3].toString();
                if(count == 10){
                    break;
                }
                context.write(new Text(biz_id), new Text(addr+ "\t" + categories + "\t" + avg));
                count++;
            }
        }
    }

    public static class SortFloatComparator extends WritableComparator {
        public SortFloatComparator() {
            super(FloatWritable.class, true);
        }
        public int compare(WritableComparable val1, WritableComparable val2) {
            FloatWritable key1 = (FloatWritable)val1;
            FloatWritable key2 = (FloatWritable)val2;
            // specify -1 for descending order
            return -1 * key1.compareTo(key2);
        }
    }

    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // get all args
        if (args.length != 3) {
            System.err.println("Usage: TopTenBusinessDetails <reviews_dir> <business_details_dir> <out_dir> ");
            System.exit(2);
        }
        // join the two files
        Job job = new Job(conf, "joinOp");
        Path tempDir = new Path(args[2] + "_temp");
        job.setJarByClass(YelpTopTenBusiness.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, ReviewsMapper.class);
        MultipleInputs.addInputPath(job,new Path(args[1]), TextInputFormat.class, BusinessDetailsMapper.class);
        job.setReducerClass(JoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(FloatWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, tempDir);
        job.waitForCompletion(true);

        // sort by average rating
        Job job2 = new Job(conf, "topTenBusinessDetails");
        job2.setJarByClass(YelpTopTenBusiness.class);

        job2.setMapperClass(SortingMapper.class);
        job2.setReducerClass(TopTenBusinessReducer.class);

        job2.setMapOutputKeyClass(FloatWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setSortComparatorClass(SortFloatComparator.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, tempDir);
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.waitForCompletion(true);

    }
}
