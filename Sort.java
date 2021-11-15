import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Sort {
    public static class FrequencySortMap extends  Mapper<Object, Text, IntWritable, Text>
    {
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException
        {
            String word = value.toString().replaceAll("[^a-z-]", "");
            int frequency = Integer.parseInt(value.toString().replaceAll("[^0-9]", ""));

            context.write(new IntWritable(frequency), new Text(word));
        }
    }

    public static class FrequencyReducer
            extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable freq, Iterable<Text> words,
                           Context context) throws IOException, InterruptedException {
            for (Text key : words) {
                context.write(freq, key);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(Sort.class);
        job.setMapperClass(FrequencySortMap.class);
        job.setCombinerClass(FrequencyReducer.class);
        job.setReducerClass(FrequencyReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
