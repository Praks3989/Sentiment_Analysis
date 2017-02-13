/**
 * @author Gaurav
 * 
 * Class to find the nearest class for the test tweet
 */

package classification;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class Classify extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), " classify ");
		job.setJarByClass(this.getClass());

		FileInputFormat.addInputPaths(job, args[0] + "_nearest"); // Iput path
																	// for the
																	// HTML file
		FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output path
																// to store
																// logarithmic
																// term
																// frequencies
		job.setMapperClass(ClassifyMap.class);
		job.setReducerClass(ClassifyReduce.class);
		job.setMapOutputKeyClass(Text.class); // Class for Mapper Output Key
		job.setMapOutputValueClass(DoubleWritable.class); // Class for Mapper Output Value
		job.setOutputKeyClass(Text.class); // Class for Reducer Output Key
		job.setOutputValueClass(Text.class); // Class for Reducer Output Value
		job.setNumReduceTasks(1);
		int st = job.waitForCompletion(true) ? 0 : 1;
		//FileSystem fs = FileSystem.get(getConf()); 
//		Path delPath = new Path(args[0] + "_nearest"); // Deleting relevant files folder
//		if (fs.exists(delPath))
//			fs.delete(delPath, true);
		
		
		return st;
		

	}

	/*
	 * Mapper to calculate logarithmic term frequency for query terms Input:
	 * Wiki tweet Output: Key - Word and tweetID, Value - 1
	 * 
	 */

	public static class ClassifyMap extends Mapper<LongWritable, Text, Text, DoubleWritable> {

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

			String[] line = lineText.toString().split("\t");
		//	String tweetID = line[0].split("&#&")[0].trim();
			String tweetClass = line[0].split("&#&")[1].trim();
			String score = line[1].trim();

			context.write(new Text(tweetClass), new DoubleWritable(Double.parseDouble(score)));

		}
	}

	/*
	 * Reducer: Collects the tokens from mapper and calculates logarithmic term
	 * frequency Formula used : 1 + log10(TermFrequency in the tweet) Output: Key
	 * - Word and TweetID , Value - Logarithmic term Frequency
	 * 
	 */

	public static class ClassifyReduce extends Reducer<Text, DoubleWritable, Text, Text> {
		//private static ArrayList<String> classScores = new ArrayList<String>();

		@Override
		public void reduce(Text word, Iterable<DoubleWritable> counts, Context context) throws IOException, InterruptedException {
			double sum = 0.0;
			// Get total count of each word's occurence in a particular file
			if (!word.toString().trim().isEmpty() || !word.toString().trim().equals(" ")) {
				for (DoubleWritable count : counts) {
					sum += count.get();
				}

				/*
				 * Calculation for Logarithmic Term Frequency
				 */
				
				// Write (Key, Value) as (Word and File, Logarithmic Term
				// Frequency)
				context.write(word, new Text(""+sum));
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
//			double max = 0.0;
//			String maxClass = "";
//			for(String score : classScores){
//				
//				double scr = Double.parseDouble(score.split(",")[1]);
//				if(scr > max){
//					max = scr;
//					maxClass = score.split(",")[0];
//				}
//			}
//			if(maxClass.trim().equals("0"))
//				maxClass = "Negative";
//			
//			if(maxClass.trim().equals("2"))
//				maxClass = "Neutral";
//			
//			if(maxClass.trim().equals("4"))
//				maxClass = "Positive";
//			
//			context.write(new Text("Tweet is classified as: "+maxClass), new Text(""));
		}
	}
}
