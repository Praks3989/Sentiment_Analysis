/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package naiveclassifier;

import java.io.IOException;
import static naiveclassifier.Constants.equal;
import static naiveclassifier.Constants.negativeTweets;
import static naiveclassifier.Constants.negativeWords;
import static naiveclassifier.Constants.neutralTweets;
import static naiveclassifier.Constants.neutralWords;
import static naiveclassifier.Constants.newLine;
import static naiveclassifier.Constants.positiveTweets;
import static naiveclassifier.Constants.positiveWords;
import static naiveclassifier.Constants.propertyFile;
import static naiveclassifier.Constants.totalTweets;
import static naiveclassifier.Constants.totalVocab;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author prakash
 */
public class NaiveClassifier {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        // TODO code application logic here
        int result = 0;
//        TrainData trainInputData = new TrainData();
//        result = ToolRunner.run(trainInputData, args);
//        writeToPropertyFile(trainInputData);
//      CalculateProbabilities calculate = new CalculateProbabilities();
//     result = ToolRunner.run(calculate, args);
        ClassifyTweet classifyTweet = new ClassifyTweet();
        result = ToolRunner.run(classifyTweet, args);
        System.exit(result);

    }

    private static void writeToPropertyFile(TrainData trainInputData) throws IOException {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path outFile = new Path(propertyFile);
        if (!fs.exists(outFile)) {
            FSDataOutputStream out = fs.create(outFile);
            out.writeBytes(totalTweets + equal + trainInputData.totalTweets() + newLine);
            out.writeBytes(totalVocab + equal + trainInputData.totalVocab() + newLine);
            out.writeBytes(positiveTweets + equal + trainInputData.positiveTweets() + newLine);
            out.writeBytes(negativeTweets + equal + trainInputData.negativeTweets() + newLine);
            out.writeBytes(neutralTweets + equal + trainInputData.neutralTweets() + newLine);
            out.writeBytes(positiveWords + equal + trainInputData.positiveWords() + newLine);
            out.writeBytes(negativeWords + equal + trainInputData.negativeWords() + newLine);
            out.writeBytes(neutralWords + equal + trainInputData.neutralWords() + newLine);
            out.close();

        }

    }

}
