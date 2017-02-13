/*
Question: 
To close the loop, you will develop a job which implements a simple batch mode
search engine. The job (Rank.java) accepts as input a user query and outputs a
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
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * @author Devdatta
 */
public class Rank extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(Rank.class);
    private static final String KEYWORDS = "keyWords";

    public static void main(String[] args) throws Exception {
        LOG.info("starting program");
        int res = ToolRunner.run(new Rank(), args);
        System.exit(res);
    }

    /*
        This will run 2 jobs, first is same as search and second will rank the output produced
        by search.
    */
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf(), "search");
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]+"_search"));
        String[] keywords = new String[args.length - 2];
        for (int i = 2; i < args.length; i++) {
            String arg = args[i];
            keywords[i - 2] = arg;
        }
        job.getConfiguration().setStrings(KEYWORDS, keywords);
        job.getConfiguration().set("dummy", "" + 3);
        job.setMapperClass(Search.Map.class);
        job.setReducerClass(Search.Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.waitForCompletion(true);

        //---Rank start here
        Job jobRank = Job.getInstance(getConf(), "Rank");
        jobRank.setJarByClass(this.getClass());
        FileInputFormat.addInputPaths(jobRank, args[1]+"_search");
        FileOutputFormat.setOutputPath(jobRank, new Path(args[1]));
        jobRank.getConfiguration().set("dummy", "" + 3);
        jobRank.setMapperClass(MapRank.class);
        jobRank.setReducerClass(ReduceRank.class);
        jobRank.setOutputKeyClass(DoubleWritable.class);
        jobRank.setOutputValueClass(Text.class);
        /*
            We set the comparator here so that job will sort as per our need
            Comparator class written below.
        */
        jobRank.setSortComparatorClass(MyComparator.class);

        //job.setMapOutputKeyClass(Text.class);
        //job.setMapOutputValueClass(Text.class);
        return jobRank.waitForCompletion(true) ? 0 : 1;
    }
    
    //same as search
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        TreeSet<String> keywords;

        @Override
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
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

        public void map(LongWritable offset, Text lineText, Mapper.Context context) throws IOException, InterruptedException {
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

    //same as search
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

    /*
        This will read output of search
        make filename as value and sum i=of tfidf as key -> so that it can be sorted
     */
    public static class MapRank extends Mapper<LongWritable, Text, DoubleWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] lparts = line.split("\t");
            context.write(new DoubleWritable(Double.parseDouble(lparts[1])), new Text(lparts[0]));
        }
    }

    //simply write values received by making filename as key and sum of tfidf as value.
    //ie. opposit operation of the one performed in map
    public static class ReduceRank extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

        @Override
        protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                LOG.info("writing: " + key.toString() + " " + val.toString());
                context.write(val, key);
            }
        }
    }

    /*
        This is the comparator that will sort DoubleWritable values in decending order.
    */
    public static class MyComparator extends WritableComparator {

        protected MyComparator() {
            super(DoubleWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            DoubleWritable k1 = (DoubleWritable) a;
            DoubleWritable k2 = (DoubleWritable) b;
            LOG.info("comparator");
            return -1 * k1.compareTo(k2);
        }

    }

}
/*


 */
