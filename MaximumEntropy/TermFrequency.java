/*
Question: 
Make a copy of DocWordCount.java, rename it TermFrequency.java, and modify
it to compute the logarithmic Term Frequency WF(t,d)  (​equation#1).
equation1: 
WF(t,d) = 1 + log​10​
(TF(t,d))  if TF(t,d) > 0, and 0 otherwise

Main class name: wordcount.TermFrequency
    no need to give class name if using jar files I have attaced.  

*/
package wordcount;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * @author Devdatta
*/

public class TermFrequency extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(TermFrequency.class);

    public static void main(String[] args) throws Exception {
        LOG.info("starting program");
        int res = ToolRunner.run(new TermFrequency(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), " wordcount ");
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /*
        Edited mapper to emit filename along with word. 
    */
    
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

        public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
            LOG.info("in mapper function");
            String line = lineText.toString();
            Text currentWord = new Text();
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            for (String word : WORD_BOUNDARY.split(line)) {
                if (word.isEmpty()) {
                    continue;
                }
                currentWord = new Text(word+"#####"+fileName);
                context.write(currentWord, one);
            }
        }

    }

    /*
        Reducer will find count of words in a file and write its log to base 10 +1 to context
        for this it needs to iterate over itrable provided by hadoop for that word and increment variable
    */
    public static class Reduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text word, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
            LOG.info("in reducer");
            int sum = 0;
            for (IntWritable count : counts) {
                sum += count.get();
            }
            
            context.write(word, new DoubleWritable(Math.log10(sum)+1));
        }
    }
}
