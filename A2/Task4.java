import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task4 {

    public static class MapSideJoinMapper extends Mapper<Object, Text, Text, IntWritable> {
        private BufferedReader fis;
        private List<String> moviesCache = new ArrayList<>();

        public void setup(Context context) throws IOException, InterruptedException {
            Path[] caches = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            for (Path path : caches) {
                String file = path.getName().toString();
                updateList(file);
            }
        }

        private void updateList(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String movie = null;
                while ((movie = fis.readLine()) != null) {
                    moviesCache.add(movie);
                }
            } catch (IOException e) {
                System.err.println("Caught exception while parsing the cached file '" + e.getLocalizedMessage());
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokensFirst = value.toString().split(",");
            for (String s : moviesCache) {
                int count = 0;
                String[] tokensSecond = s.split(",");

                if (tokensFirst[0].equals(tokensSecond[0]))
                    continue;   // skip same names

                for (int j = 1; j < Math.min(tokensFirst.length, tokensSecond.length); j++) {
                    if (!tokensFirst[j].equals("") && tokensFirst[j].equals(tokensSecond[j]))
                        count++;
                }

                context.write(new Text(tokensFirst[0].compareTo(tokensSecond[0]) > 0 ?
                                tokensSecond[0] + "," + tokensFirst[0] : tokensFirst[0] + "," + tokensSecond[0]),
                        new IntWritable(count));
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, values.iterator().next());
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");
//        DistributedCache.addCacheFile(new URI(otherArgs[0]), conf);

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // add code here
        if (otherArgs.length != 2) {
            System.err.println("Usage: Task4 <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Task4");
        job.setJar("Task4.jar");

        job.setMapperClass(MapSideJoinMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.addCacheFile(new URI(otherArgs[0]));

        TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
        TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}