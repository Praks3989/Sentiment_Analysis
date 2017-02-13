/*
Question: 
To close the loop, you will develop a job which implements a simple batch mode
search engine. The job (Search.java) accepts as input a user query and outputs a
list of documents with scores that best matches the query (a.k.a search hits​). The
map and reduce phases of this job are explained below.

   while running code: ip-path op-path <list of query words>
Main class name: wordcount.TFIDF
    no need to give class name if using jar files I have attaced.  
 */
package wordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * @author Devdatta
 */
public class Search extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(Search.class);
    private static final String KEYWORDS = "keyWords";

    public static void main(String[] args) throws Exception {
        LOG.info("starting program");
        int res = ToolRunner.run(new Search(), args);
        System.exit(res);
    }

    /*
        This will take words passed as argument on command line and append it to the 
        configuration as array of strings.
    */
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "search");
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        String[] keywords = new String[args.length - 2];
        for (int i = 2; i < args.length; i++) {
            String arg = args[i];
            keywords[i - 2] = arg;
        }
        job.getConfiguration().setStrings(KEYWORDS, keywords);
        job.getConfiguration().set("dummy", "" + 3);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /*
        Mapper will go through each line and check if keyword is present or not.
        if present it will write file as key and tfidf as value to context
     */
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        TreeSet<String> keywords;

        /*
            Setup method.. read all keywords here which have to be searched..
            Executes only once before map calls
        */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String[] k = context.getConfiguration().getStrings(KEYWORDS);
            keywords = new TreeSet<>();
            if (k != null) {
                for (int i = 0; i < k.length; i++) {
                    String string = k[i];
                    keywords.add(string);
                }
            } else {
                keywords.add("null");
            }
        }

        /*
            Check for keyword, and emit filename and tfidf.
        */
        public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
            LOG.info("in mapper function");
            String line = lineText.toString();
            //context.write(new Text("----input----"), new Text(line));
            String[] lpatrts = line.split("#####");
            if (keywords.contains(lpatrts[0])) {
                //context.write(new Text("----found keyword----"), new Text(lpatrts[0]));
                String[] toRet = lpatrts[1].split("\t");
                context.write(new Text(toRet[0]), new Text(toRet[1]));
            } else {
                // context.write(new Text("----no keyword----"), new Text(lpatrts[0]));
            }
        }

    }
    
    /*
        It will receive all tfidf values associated with file, add them and emit with file name. 
    */

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            LOG.info("in reducer");
            double sum = 0;
            for (Text value : values) {
                try {
                    sum += Double.parseDouble(value.toString());
                } catch (Exception e) {
                    context.write(key, value);
                }
            }
            Text t = new Text("" + sum);
            context.write(key, t);

        }
    }

}
/*
String line = lineText.toString();
            String[] lparts = line.split("\t");
            context.write(new Text(lparts[1]), new Text(lparts[0]));

for (Text val : values) {
                context.write(val, key);
            }

 */
