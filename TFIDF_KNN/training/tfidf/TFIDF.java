/**
 * @author Gaurav Dhamdhere
 * 
 * 
 * TFIDF
 * 
 * Calculate TFIDF scores for each word based on it term frequency
 */


package tfidf;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class TFIDF extends Configured implements Tool {

	

	

	public int run(String[] args) throws Exception {
		
			
			Job job = Job.getInstance(getConf(), " tfidf ");
			job.setJarByClass(this.getClass());
			
			NLineInputFormat.addInputPaths(job, args[0]+"_tf"); // Path to TermFrequency output
			job.getConfiguration().set("docNumber", args[2]);
			FileOutputFormat.setOutputPath(job, new Path(args[1])); //path for TFIDF output
			job.setMapperClass(TFIDFMap.class);
			job.setReducerClass(TFIDFReduce.class);
			job.setMapOutputKeyClass(Text.class); 			// Class for Mapper Output  Key
			job.setMapOutputValueClass(Text.class); 		// Class for Mapper Output Value
			job.setOutputKeyClass(Text.class); 			// Class for Reducer Output Key
			job.setOutputValueClass(DoubleWritable.class);	// Class for Reducer Output Value

			return job.waitForCompletion(true) ? 0 : 1;

		
	}



	/*
	 *  Mapper takes input as Term frequencies for each word in a each document
	 *  and calculates TFIDF scores for them
	 *  Output: Key - Word, Value - TweetID and Term Frequency	
	 */
	
	public static class TFIDFMap extends Mapper<LongWritable, Text, Text, Text> {


	

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

			String line = lineText.toString();
			

			String key1 = line.split("&#FILESEP#&")[0]; // get word
			String value1 = line.split("&#FILESEP#&")[1].split("\t")[0]; // get filename
			String value2 = line.split("&#FILESEP#&")[1].split("\t")[1]; // get term frequency
			Text currentWord = new Text(key1);
			Text valueWord = new Text(value1 + "##&&RSEP&&##" + value2);
			
			//Write (Key,Value) as (Word, TweetID and Term Frequency)
			context.write(currentWord, valueWord);

		}
	}

	/*
	 * Reducer: Collects the TermFrequencies from mapper in sorted order and calculates IDF and TFIDF.
	 * Formula used for calculating IDF(w): log10(1 + (Total #Files / #Files with the w))
	 * Output: Key - Word and TweetID, Value - TFIDF score
	 */		
	public static class TFIDFReduce extends Reducer<Text, Text, Text, DoubleWritable> {

		@Override
		public void reduce(Text word, Iterable<Text> postings, Context context)
				throws IOException, InterruptedException {
			long fileCount = context.getConfiguration().getLong("docNumber", 1);	// Number of files counted in TermFrequency job
			String fname, tfscore;
			int i = 0;
			ArrayList<Text> cache = new ArrayList<Text>();
			for (Text term : postings) {		// Calculate total number of files in which the word appears
				i++;
				Text cache_obj = new Text(term);
				cache.add(cache_obj);
			}

			double logarithmicSum = Math.log10(1 + (fileCount / i));	// IDF Calculation

			for (Text term : cache) {
				fname = term.toString().split("##&&RSEP&&##")[0];
				tfscore = term.toString().split("##&&RSEP&&##")[1];
				double tf_double = Double.parseDouble(tfscore);		// Get term frequency 
				double tf_idf = tf_double * logarithmicSum;				//TF-IDF Calculation		
				Text newKey = new Text(word.toString() + "&#FILESEP#&" + fname);
				
				//Output (Key, Value) is (Word and TweetID, TFIDF score)
				context.write(newKey, new DoubleWritable(tf_idf));
			}

		}
	}

}
