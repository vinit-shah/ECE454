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

public class Task1 {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable zero = new IntWritable(0);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");

            word.set(tokens[0]);    // set movie name as key
            int max = 0;            // lowest rating default: assume at least one non-blank rating
            int count = 1;          // count num of max

            // find the max first
            for (int i = 1; i < tokens.length; i++) {      // go through all available user ratings
                if (tokens[i].length() > 0 && Integer.parseInt(tokens[i]) > max) {
                    max = Integer.parseInt(tokens[i]);
                    count = 1;
                } else if (tokens[i].length() > 0 && Integer.parseInt(tokens[i]) == max) {
                    count++;
                }
            }

            // find all users with this max
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i < tokens.length; i++) {
                if (tokens[i].length() > 0 && Integer.parseInt(tokens[i]) == max) {
                    sb.append(i);
                    count--;

                    if (count > 0)
                        sb.append(',');
                }
            }

            context.write(word, new Text(sb.toString()));
        }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            result.set(values.iterator().next());
            context.write(key, result);   // each key will have one value
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

        Job job = Job.getInstance(conf, "Task1");
        // one of the following two lines is often needed
        // to avoid ClassNotFoundException
//        job.setJarByClass(Task1.class);
        job.setJar("Task1.jar");

        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
        TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}