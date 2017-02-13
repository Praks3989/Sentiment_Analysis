/**
 * @author Gaurav Dhamdhere
 * 
 * 
 * TermFrequency
 * 
 * Calculates the logarithmic term frequency for words found inside Text tag of tweets
 */
package tfidf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

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

import preprocessing.TweetCleaner;

public class TermFrequency extends Configured implements Tool {
	public long nodeCount = 0;

	public int run(String[] args) throws Exception {

		Job job = Job.getInstance(getConf(), " termfrequency ");
		job.setJarByClass(this.getClass());

		FileInputFormat.addInputPaths(job, args[0]); // Input path for the HTML
														// file
		FileOutputFormat.setOutputPath(job, new Path(args[0] + "_tf")); // Output
																		// path
																		// to
																		// store
																		// logarithmic
																		// term
																		// frequencies
		job.setMapperClass(TFMap.class);
		job.setReducerClass(TFReduce.class);
		job.setMapOutputKeyClass(Text.class); // Class for Mapper Output Key
		job.setMapOutputValueClass(Text.class); // Class for Mapper Output Value
		job.setOutputKeyClass(Text.class); // Class for Reducer Output Key
		job.setOutputValueClass(Text.class); // Class for Reducer Output Value

		int status = job.waitForCompletion(true) ? 0 : 1;
		nodeCount = job.getCounters().findCounter("Val", "cntVal").getValue();
		// System.out.println("Number of Files found--------" + nodeCount); //
		// get total file count

		return status;

	}

	/*
	 * Mapper to calculate logarithmic term frequency for query terms Input:
	 * Wiki File Output: Key - Word and TweetID, Value - 1
	 * 
	 */

	public static class TFMap extends Mapper<LongWritable, Text, Text, Text> {
		private static ArrayList<String> stopwords;
		protected void setup(Context context) throws IOException, InterruptedException {
			/*
			 * Stopwords List
			 * */
		String[] sw = { "a", "about", "above", "above", "across", "after", "afterwards", "again",
				"against", "all", "almost", "alone", "along", "already", "also", "although", "always", "am", "among",
				"amongst", "amoungst", "amount", "an", "and", "another", "any", "anyhow", "anyone", "anything",
				"anyway", "anywhere", "are", "around", "as", "at", "back", "be", "became", "because", "become",
				"becomes", "becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides",
				"between", "beyond", "bill", "both", "bottom", "but", "by", "call", "can", "cannot", "cant", "co",
				"con", "could", "couldnt", "cry", "de", "describe", "detail", "do", "done", "down", "due", "during",
				"each", "eg", "eight", "either", "eleven", "else", "elsewhere", "empty", "enough", "etc", "even",
				"ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fify", "fill",
				"find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from", "front",
				"full", "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here",
				"hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how",
				"however", "hundred", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its",
				"itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me",
				"meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must",
				"my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody",
				"none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one",
				"only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own",
				"part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem", "seemed", "seeming",
				"seems", "serious", "several", "she", "should", "show", "side", "since", "sincere", "six", "sixty",
				"so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still", "such",
				"system", "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence",
				"there", "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thickv",
				"thin", "third", "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to",
				"together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", "until", "up",
				"upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence",
				"whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether",
				"which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with",
				"within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves" };
		stopwords = new ArrayList<String>(Arrays.asList(sw));
		}

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

			// String[] line = lineText.toString().split(",");
			String[] line = lineText.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
			String tweetID = line[1].trim();
			String tweetTextRaw = line[5];
			String tweetClass = line[0].trim();
			String tweetText;
			try {
                tweetText = TweetCleaner.clean(tweetTextRaw);
            } catch (Exception e) {
                tweetText = tweetTextRaw;
            }

			for (String tweet : tweetText.split(" ")) {
				if(!stopwords.contains(tweet) && !tweet.isEmpty())
					context.write(new Text(tweet.trim() + "&#FILESEP#&" + tweetID + "&#&" + tweetClass), new Text("1"));
				// Increment Counter. This is to count the number of Files
			}
			context.getCounter("Val", "cntVal").increment(1);

		}
	}

	/*
	 * Reducer: Collects the tokens from mapper and calculates logarithmic term
	 * frequency Formula used : 1 + log10(TermFrequency in the file) Output: Key
	 * - Word and TweetID , Value - Logarithmic term Frequency
	 * 
	 */

	public static class TFReduce extends Reducer<Text, Text, Text, DoubleWritable> {
		@Override
		public void reduce(Text word, Iterable<Text> counts, Context context) throws IOException, InterruptedException {
			int sum = 0;
			// Get total count of each word's occurence in a particular tweet
			if (!word.toString().trim().isEmpty() || !word.toString().trim().equals(" ")) {
				for (Text count : counts) {
					sum += 1;
				}

				/*
				 * Calculation for Logarithmic Term Frequency
				 */
				double logarithmicSum = 1.0 + Math.log10(sum);
				// Write (Key, Value) as (Word and TweetID, Logarithmic Term
				// Frequency)
				context.write(word, new DoubleWritable(logarithmicSum));
			}
		}
	}
}
