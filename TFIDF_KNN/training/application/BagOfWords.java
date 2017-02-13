/**
 * @author Gaurav Dhamdhere
 *
 * 
 * Main class for training the classifier over Training Data
 */
package application;

import org.apache.hadoop.util.ToolRunner;


import tfidf.TFIDF;
import tfidf.TermFrequency;


public class BagOfWords {

	public static void main(String[] args) throws Exception {
		TermFrequency termFrequency = new TermFrequency();
		int res = ToolRunner.run(termFrequency, args); // Find Term Frequency
		if (res == 0) {
			String[] parameters = { args[0], args[1],"" + termFrequency.nodeCount };
			res = ToolRunner.run(new TFIDF(), parameters); // Find TFIDF
			
		}
		System.exit(res);

	}

}
