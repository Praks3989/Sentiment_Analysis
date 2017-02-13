/**
 * @author Gaurav 
 * 
 * 
 * Class to classify the test tweet into appropriate class
 */
package application;

import org.apache.hadoop.util.ToolRunner;

import search.Search_Rank;

import classification.Classify;

public class BagOfWords {

	public static void main(String[] args) throws Exception {
		
		
			
			
				int res = ToolRunner.run(new Search_Rank(), args); // Search and
																// Rank matching
																// documents
																// based on the
																// query
				if (res == 0) {
					res = ToolRunner.run(new Classify(), args); // Classify
				}
			
		
		System.exit(res);

	}

}
