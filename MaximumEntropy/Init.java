package wordcount;

import java.io.IOException;
import java.util.HashMap;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

/**
 * @author Devdatta
 */
public class Init extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(Init.class);

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), " init ");
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Init.Map.class);
        job.setReducerClass(Init.Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);
        return (int) job.getCounters().findCounter("totalCount", "cnt").getValue();
    }

    /*
        Edited mapper to emit filename along with word. Reducer not thanged
     */
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

        public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
            String line = lineText.toString();
            String tweet;
            String[] splitted;
            splitted = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            try {
                tweet = TweetCleaner.cleanTweet(splitted[5]);
            } catch (Exception e) {
                tweet = splitted[5];
            }
            String tweetId = splitted[1].replaceAll("\"", "");
            String tweetClass = splitted[0].replaceAll("\"", "");

            HashMap<String, Integer> countMap = new HashMap();
            String[] lparts = tweet.split("\\s*\\b\\s*");
            for (String word : lparts) {
                if (countMap.containsKey(word)) {
                    countMap.put(word, countMap.get(word) + 1);
                } else {
                    countMap.put(word, 1);
                }
            }

            for (java.util.Map.Entry<String, Integer> entry : countMap.entrySet()) {
                String word = entry.getKey();
                Integer count = entry.getValue();
                context.write(new Text(tweetId), new Text(word + Main.FIELD_SEP + tweetClass + Main.FIELD_SEP + count + Main.FIELD_SEP + 0));
            }
        }
    }

    /*
        Reducer will simply count and write to context
     */
    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text word, Iterable<Text> counts, Context context) throws IOException, InterruptedException {
            StringBuffer buffer = new StringBuffer();
            String prefix = "";
            for (Text count : counts) {
                buffer.append(prefix);
                prefix = Main.RECORD_SEP;
                buffer.append(count.toString());
                //total docs
                context.getCounter("totalCount", "cnt").increment(1);
            }
            context.write(word, new Text(buffer.toString()));
        }
    }
}
