/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package naiveclassifier;

/**
 *
 * @author prakash
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Pattern;
import static naiveclassifier.Constants.comma;
import static naiveclassifier.Constants.delimiter;
import static naiveclassifier.Constants.space;
import static naiveclassifier.Constants.stopWords;
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

public class TrainData extends Configured implements Tool {
    
    static Job createInputData;
    
    @Override
    public int run(String[] args) throws Exception {
        
        createInputData = Job.getInstance(getConf(), " Create Input Data ");
        createInputData.setJarByClass(this.getClass());
        
        FileInputFormat.addInputPaths(createInputData, args[0]);
        FileOutputFormat.setOutputPath(createInputData, new Path(args[1] + "_temp"));
        createInputData.setMapperClass(Map.class);
        createInputData.setReducerClass(Reduce.class);
        //Enforcing single reducer to count the no of nodes.
        createInputData.setNumReduceTasks(1);
        createInputData.setMapOutputKeyClass(Text.class);
        createInputData.setMapOutputValueClass(Text.class);
        createInputData.setOutputKeyClass(Text.class);
        createInputData.setOutputValueClass(IntWritable.class);
        boolean success = createInputData.waitForCompletion(true);
        if (success) {
            return 0;
        } else {
            return 1;
        }
    }
    
    public long totalTweets() throws IOException {
        return createInputData.getCounters().findCounter(EnumCounters.TOTAL_TWEEETS).getValue();
    }
    
    public long totalVocab() throws IOException {
        return createInputData.getCounters().findCounter(EnumCounters.VOCABULARY).getValue();
    }
    
    public long positiveTweets() throws IOException {
        return createInputData.getCounters().findCounter(EnumCounters.TOTAL_POSITIVE_TWEETS).getValue();
    }
    
    public long negativeTweets() throws IOException {
        return createInputData.getCounters().findCounter(EnumCounters.TOTAL_NEGATIVE_TWEETS).getValue();
    }
    
    public long neutralTweets() throws IOException {
        return createInputData.getCounters().findCounter(EnumCounters.TOTAL_NEUTRAL_TWEETS).getValue();
    }
    
    public long positiveWords() throws IOException {
        return createInputData.getCounters().findCounter(EnumCounters.TOTAL_POSITIVE_WORDS).getValue();
    }
    
    public long negativeWords() throws IOException {
        return createInputData.getCounters().findCounter(EnumCounters.TOTAL_NEGATIVE_WORDS).getValue();
    }
    
    public long neutralWords() throws IOException {
        return createInputData.getCounters().findCounter(EnumCounters.TOTAL_NEUTRAL_WORDS).getValue();
    }
    
    /*
    Input: Training data of the form:
    "0","1467810369","Mon Apr 06 22:19:45 PDT 2009","NO_QUERY","_TheSpecialOne_","@switchfoot http://twitpic.com/2y1zl - Awww, that's a bummer.  You shoulda got David Carr of Third Day to do it. ;D"
    Output:WORD TWEET_TYPE
    
    

    */
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        // Declaring String constants for parsing the input file.
        private static final String EXTRACT_TWEET = ",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)";
        private static ArrayList<String> stopwordsList = new ArrayList<String>(Arrays.asList(stopWords));
        
        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {
            
            String line = lineText.toString();
            if (!line.isEmpty()) {
                String[] parts = line.split(EXTRACT_TWEET);
                String tweet = TweetCleaner.cleanTweet(parts[5]);
                context.getCounter(EnumCounters.TOTAL_TWEEETS).increment(1);

                //String tweet = parts[1].replaceAll("\"", "");
                String tweetType = parts[0].replaceAll("\"", "");
                String[] wordList = tweet.split(space);
                for (int iter = 0; iter < wordList.length; iter++) {
                    //Emit word \t class
                    wordList[iter].trim();
                    if (!wordList[iter].isEmpty() && !stopwordsList.contains(wordList[iter].toLowerCase())) {
                        context.write(new Text(wordList[iter]), new Text(tweetType));
                    }
                }
                switch (Integer.parseInt(tweetType)) {
                    case 0:
                        context.getCounter(EnumCounters.TOTAL_NEGATIVE_TWEETS).increment(1);
                        break;
                    case 2:
                        context.getCounter(EnumCounters.TOTAL_NEUTRAL_TWEETS).increment(1);
                        break;
                    case 4:
                        context.getCounter(EnumCounters.TOTAL_POSITIVE_TWEETS).increment(1);
                        break;
                    default:
                        break;
                }
                
            }
            
        }
    }
    
    /*
    Input:
    Output:WORD####TWEET_CLASS COUNT
    
    
    */
    public static class Reduce extends Reducer< Text, Text, Text, IntWritable> {

        //int noOfNodes=0;
        private final static IntWritable NEGATIVE = new IntWritable(0);
        private final static IntWritable POSITIVE = new IntWritable(4);
        private final static IntWritable NEUTRAL = new IntWritable(2);
        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            
            context.getCounter(EnumCounters.VOCABULARY).increment(1);
            int negativeCounters = 0, positiveCounters = 0, neutralCounters = 0;
            for (Text iter : values) {
                
                switch (Integer.parseInt(iter.toString())) {
                    case 0:
                        context.getCounter(EnumCounters.TOTAL_NEGATIVE_WORDS).increment(1);
                        negativeCounters++;
                        break;
                    case 2:
                        context.getCounter(EnumCounters.TOTAL_NEUTRAL_WORDS).increment(1);
                        neutralCounters++;
                        break;
                    case 4:
                        context.getCounter(EnumCounters.TOTAL_POSITIVE_WORDS).increment(1);
                        positiveCounters++;
                        break;
                    default:
                        break;
                }
                
            }
            context.write(new Text(key + delimiter + NEGATIVE), new IntWritable(negativeCounters));
            context.write(new Text(key + delimiter + NEUTRAL), new IntWritable(neutralCounters));
            context.write(new Text(key + delimiter + POSITIVE), new IntWritable(positiveCounters));
            
        }
        
    }
    
}
