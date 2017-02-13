/*
Question: 
Make a copy of the file TermFrequency.java, rename it TFIDF.java, and modify it
so it runs two mapreduce jobs, one after another. The first mapreduce job
computes the Term Frequency as described above. The second job takes the
output files of the first job as input and computes TF­IDF values. The map and
reduce phases of the second pass are explained below.

    RUN THIS ON INPUT DATA-- NOT ON INTERMEDIATE OUTPUT OF DocWordCount
-> TO store intermediate output, a separate directory will be created by appending 

Main class name: wordcount.TFIDF
    no need to give class name if using jar files I have attaced.  

 */
package wordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
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
public class TFIDF extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(TFIDF.class);
    private static final String TEMPDIR = "temp";
    private static final String FILECOUNT = "fileCount";

    public static void main(String[] args) throws Exception {
        LOG.info("starting program");
        int res = ToolRunner.run(new TFIDF(), args);
        System.exit(res);
    }

    /*
        Appended two jobs to each other
    */
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), " wordcount ");
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1] + TEMPDIR));
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.waitForCompletion(true);

        /*
            These lines will find count of input files in input directory and add it
            to the context object. Job can read the count from this context object.
        */
        FileSystem fs = FileSystem.get(getConf());
        Path pt = new Path(args[0]);
        ContentSummary cs = fs.getContentSummary(pt);
        long inputFiles = cs.getFileCount();
        getConf().set(FILECOUNT, ""+inputFiles);
        
        Job idfJob = Job.getInstance(getConf(), "IDF");
        idfJob.setJarByClass(this.getClass());
        FileInputFormat.addInputPaths(idfJob, args[1] + TEMPDIR);
        FileOutputFormat.setOutputPath(idfJob, new Path(args[1]));
        idfJob.setMapperClass(MapForIDF.class);
        idfJob.setReducerClass(ReduceForIDF.class);
        idfJob.setOutputKeyClass(Text.class);
        idfJob.setOutputValueClass(Text.class);

        return idfJob.waitForCompletion(true) ? 0 : 1;
    }

    /*
        Edited mapper to emit filename along with word. -> same pammer as that of doc word count
     */
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

        public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
            LOG.info("in mapper function");
            String line = lineText.toString();
            Text currentWord = new Text();
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            for (String word : WORD_BOUNDARY.split(line)) {
                if (word.isEmpty()) {
                    continue;
                }
                currentWord = new Text(word + "#####" + fileName);
                context.write(currentWord, one);
            }
        }

    }

    /*
        Same mapper as that of Doc word count
    */
    public static class Reduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text word, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
            LOG.info("in reducer");
            int sum = 0;
            for (IntWritable count : counts) {
                sum += count.get();
            }

            context.write(word, new DoubleWritable(Math.log10(sum) + 1));
        }
    }

    /*
        This is mapper for second MR job.
        This will get each line from previous output and split into word and metadata.
        Metadata will contain file name and TF separated by tab
    
    */
    public static class MapForIDF extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

        public void map(LongWritable offset, Text lineText, Mapper.Context context) throws IOException, InterruptedException {
            LOG.info("in second mapper function");
            String line = lineText.toString();
            if (line.isEmpty()) {
                return;
            }
            String[] lineparts = line.split("#####");
            if (lineparts.length < 2) {
                return;
            }
            Text currentWord = new Text(lineparts[0]);
            Text wordDetails = new Text(lineparts[1]);
            context.write(currentWord, wordDetails);
        }

    }

    /*
        This is reducer for TF-IDF.
        It will receive the 
    */
    public static class ReduceForIDF extends Reducer<Text, Text, Text, DoubleWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //This is not so good approach to receive parameter but works fine. -> the way performed in Search.java
            //Ideally below operation should be done in "setup(Context c)" method, 
            //as we want this operation to run only once before job starts. 
            long fCount = context.getConfiguration().getLong(FILECOUNT, 1);
            double idf = 0;
            //this list will store all files provided by iterator. We need this intermediate step
            //as we need to know the no of files containing word.
            ArrayList<Text> filesHavingWord = new ArrayList<>();
            for (Text text : values) {
                filesHavingWord.add(new Text(text.toString()));
            }
            //This loop will actually traverse over above saved file list and calculate idf.
            //this will also multiply tf with idf and write result to context
            for (Text fileHavingWord : filesHavingWord) {
                String[] sparts = fileHavingWord.toString().split("\t");
                double tfIdf =  0;
                tfIdf = Double.parseDouble(sparts[1])* Math.log10(1 +fCount/filesHavingWord.size());
                context.write(new Text(key.toString() + "#####" + sparts[0]), new DoubleWritable(tfIdf));
            }

        }

    }
}
