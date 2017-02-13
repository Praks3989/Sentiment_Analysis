/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package naiveclassifier;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import static naiveclassifier.Constants.delimiter;
import static naiveclassifier.Constants.equal;
import static naiveclassifier.Constants.negativeTweets;
import static naiveclassifier.Constants.neutralTweets;
import static naiveclassifier.Constants.positiveTweets;
import static naiveclassifier.Constants.propertyFile;
import static naiveclassifier.Constants.space;
import static naiveclassifier.Constants.tab;
import static naiveclassifier.Constants.totalTweets;
import static naiveclassifier.Constants.userTweet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 *
 * @author prakash
 */
public class ClassifyTweet extends Configured implements Tool {

    static Job classifyTweet;

    @Override
    public int run(String[] args) throws Exception {

        classifyTweet = Job.getInstance(getConf(), " Create Input Data ");
        classifyTweet.setJarByClass(this.getClass());

        //FileInputFormat.addInputPaths(classifyTweet, args[1] + "_temp");
        FileInputFormat.addInputPaths(classifyTweet, "output_temp1");
        FileOutputFormat.setOutputPath(classifyTweet, new Path(args[1]));
        classifyTweet.setMapperClass(Map.class);
        classifyTweet.setReducerClass(Reduce.class);
        //Enforcing single reducer to count the no of nodes.
        classifyTweet.setNumReduceTasks(1);
        classifyTweet.setMapOutputKeyClass(IntWritable.class);
        classifyTweet.setMapOutputValueClass(DoubleWritable.class);
        classifyTweet.setOutputKeyClass(IntWritable.class);
        classifyTweet.setOutputValueClass(DoubleWritable.class);
        setConfiguration(classifyTweet, args);
        boolean success = classifyTweet.waitForCompletion(true);
        if (success) {
            return 0;
        } else {
            return 1;
        }
    }

    private void setConfiguration(Job classifyTweet, String[] args) throws IOException {
        // Remove file hardcoding
        Path pt = new Path(propertyFile);
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line;
        line = br.readLine();
        while (line != null) {
            String[] parts = line.split(equal);
            classifyTweet.getConfiguration().set(parts[0], parts[1]);
            //System.out.println("\n\n\n" + parts[0] + "\t" + parts[1] + "\n\n\n");
            line = br.readLine();
        }
        String uncleanTweet = "";
        for (int i = 2; i < args.length; i++) {
            uncleanTweet += args[i] + " ";
        }
        String cleanTweet = TweetCleaner.cleanTweet(uncleanTweet);
        classifyTweet.getConfiguration().set(userTweet, cleanTweet);

    }

    /*
    Input:WORD####TWEET_CLASS COUNT
    Output:TWEET_CLASS PROBABILITY
     */
    public static class Map extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {
            IntWritable key;
            DoubleWritable value;
            String[] wordList = lineText.toString().split(tab);
            String completeTweet = context.getConfiguration().get(userTweet);
            String[] tweetWord = completeTweet.trim().split(space);
            for (int i = 0; i < tweetWord.length; i++) {
                for (int countClass = 0; countClass < 3; countClass++) {
                    tweetWord[i].trim();
                    if (!tweetWord[i].isEmpty()) {
                        if ((tweetWord[i] + delimiter + (countClass * 2)).equalsIgnoreCase(wordList[0])) {
                            key = new IntWritable(countClass * 2);
                            value = new DoubleWritable(Double.parseDouble(wordList[1]));
                            context.write(key, value);
                        }
                    }

                }

            }

        }
    }

    /*
    Input:TWEET_CLASS PROBABILITY
    Output:CLASSIFIED_TWEET_CLASS FINAL_PROBABILTY
     */
    public static class Reduce extends Reducer< IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

        static int finalClass = -1;
        static double prevProbability = 0.0;

        @Override
        public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double classProbability = 0.0;
            double finalProbability = 1.0, negativeProbability = 1.0, neutralProbability = 1.0, positiveProbability = 1.0;
            for (DoubleWritable iter : values) {
                double value = Double.parseDouble(iter.toString());
                switch (Integer.parseInt(key.toString())) {
                    case 0:
                        classProbability = (double) Long.parseLong(context.getConfiguration().get(negativeTweets)) / (double) Long.parseLong(context.getConfiguration().get(totalTweets));
                        negativeProbability = finalProbability * value;
                        break;
                    case 2:
                        classProbability = (double) Long.parseLong(context.getConfiguration().get(neutralTweets)) / (double) Long.parseLong(context.getConfiguration().get(totalTweets));
                        neutralProbability = finalProbability * value;
                        break;
                    case 4:
                        classProbability = (double) Long.parseLong(context.getConfiguration().get(positiveTweets)) / (double) Long.parseLong(context.getConfiguration().get(totalTweets));
                        //context.write(new IntWritable(5),new DoubleWritable(classProbability));
                        positiveProbability = finalProbability * value;
                        //context.write(new IntWritable(6),new DoubleWritable(finalProbability));
                        break;
                    default:
                        break;
                }

            }
            finalProbability = neutralProbability * negativeProbability * positiveProbability * classProbability;
            if (prevProbability < finalProbability) {
                finalClass = Integer.parseInt(key.toString());
                prevProbability = finalProbability;

            }

            //context.write(key, new DoubleWritable(finalProbability * classProbability));
        }

        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            //Text t1 = new Text("Total Count");
            context.write(new IntWritable(finalClass), new DoubleWritable(prevProbability));
        }
    }
}
