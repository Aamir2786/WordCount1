import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;


public class WordCount {

    public  static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final IntWritable InitialCount = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

            String line = value.toString();
            StringTokenizer str = new StringTokenizer(line);
            while(str.hasMoreElements()){
                word.set(str.nextToken());
                context.write(word, InitialCount);
            }
        }
    }
    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

            int sum=0;

            for(IntWritable i : values){
                sum+=i.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String args[]) throws Exception {
        Job job = Job.getInstance(new Configuration(), "Word Count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
