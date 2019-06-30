import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task3 {

    public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable zero = new IntWritable(0);
        private IntWritable user = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");

            // Count commas to know user count
            String val = value.toString();
            int userCount = val.length() - val.replace(",", "").length();   // # of commas = users
//            System.out.println("User count: " + userCount);

            // Start from index 1 as the first one is movie name
            for (int i = 1; i <= userCount; i++) {
                user.set(i);
                if (i < tokens.length) {
//                    System.out.println("User " + i + " \"" + tokens[i] + "\"");
                    context.write(user, tokens[i].length() > 0 ? one : zero);
                } else {
                    context.write(user, zero);
                }
            }

            // If any empty user entries
//            for (int i = )
        }
    }

    public static class IntSumReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // add code here
        if (otherArgs.length != 2) {
            System.err.println("Usage: task3 <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Task3");
        // one of the following two lines is often needed
        // to avoid ClassNotFoundException
//        job.setJarByClass(Task3.class);
        job.setJar("Task3.jar");

        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
        TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}