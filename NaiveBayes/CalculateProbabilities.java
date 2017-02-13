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
import static naiveclassifier.Constants.negativeWords;
import static naiveclassifier.Constants.neutralWords;
import static naiveclassifier.Constants.positiveWords;
import static naiveclassifier.Constants.propertyFile;
import static naiveclassifier.Constants.tab;
import static naiveclassifier.Constants.totalVocab;
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
public class CalculateProbabilities extends Configured implements Tool {

    static Job calculateProbabilities;

    @Override
    public int run(String[] args) throws Exception {

        calculateProbabilities = Job.getInstance(getConf(), " Create Input Data ");
        calculateProbabilities.setJarByClass(this.getClass());

        //FileInputFormat.addInputPaths(calculateProbabilities, args[1] + "_temp");
        FileInputFormat.addInputPaths(calculateProbabilities, "output_temp");
        FileOutputFormat.setOutputPath(calculateProbabilities, new Path(args[1] + "_temp1"));
        calculateProbabilities.setMapperClass(CalculateProbabilities.Map.class);
        calculateProbabilities.setReducerClass(CalculateProbabilities.Reduce.class);
        //Enforcing single reducer to count the no of nodes.
        calculateProbabilities.setNumReduceTasks(1);
        calculateProbabilities.setMapOutputKeyClass(Text.class);
        calculateProbabilities.setMapOutputValueClass(IntWritable.class);
        calculateProbabilities.setOutputKeyClass(Text.class);
        calculateProbabilities.setOutputValueClass(DoubleWritable.class);
        setConfiguration(calculateProbabilities);
        boolean success = calculateProbabilities.waitForCompletion(true);
        if (success) {
            return 0;
        } else {
            return 1;
        }
    }

    private void setConfiguration(Job calculateProbabilities) throws IOException {
        // Remove file hardcoding
        Path pt = new Path(propertyFile);
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line;
        line = br.readLine();
        while (line != null) {
            String[] parts = line.split(equal);
            calculateProbabilities.getConfiguration().set(parts[0], parts[1]);
            //System.out.println("\n\n\n" + parts[0] + "\t" + parts[1] + "\n\n\n");
            line = br.readLine();
        }

    }

    /*
    Input:WORD####TWEET_CLASS COUNT
    Output:WORD####TWEET_CLASS COUNT
     */
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        // Declaring String constants for parsing the input file.

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {

            String line = lineText.toString();
            if (!line.isEmpty()) {
                String[] parts = line.split(tab);
                context.write(new Text(parts[0]), new IntWritable(Integer.parseInt(parts[1])));

            }
        }
    }

    /*
    Input:WORD####TWEET_CLASS COUNT
    Output:WORD####TWEET_CLASS PROBABILITY
     */
    public static class Reduce extends Reducer< Text, IntWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int frequency = 0;
            for (IntWritable iter : values) {
                frequency = Integer.parseInt(iter.toString());
            }
            String wordAndClass = key.toString();
            String[] parts = wordAndClass.split(delimiter);
            String category = parts[1];
            double conditionalProbability = 0.0;
            long vocab = Long.parseLong(context.getConfiguration().get(totalVocab));

            switch (Integer.parseInt(category)) {
                case 0:
                    long negative = Long.parseLong(context.getConfiguration().get(negativeWords));
                    conditionalProbability = ((double) (frequency + 1) / (double) (vocab + negative));
                    break;
                case 2:
                    long neutral = Long.parseLong(context.getConfiguration().get(neutralWords));
                    conditionalProbability = ((double) (frequency + 1) / (double) (vocab + neutral));
                    break;
                case 4:
                    long positive = Long.parseLong(context.getConfiguration().get(positiveWords));
                    conditionalProbability = ((double) (frequency + 1) / (double) (vocab + positive));
                    break;
                default:
                    break;
            }

            context.write(key, new DoubleWritable(conditionalProbability));

        }

    }
}
