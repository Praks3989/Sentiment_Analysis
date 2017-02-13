package wordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * @author Devdatta
 */
public class DocWordCount extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(DocWordCount.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new DocWordCount(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), " wordcount ");
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /*
        read line, split on tab, write sentiment value and word to context
     */
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
            String line = lineText.toString();
            Text currentWord = new Text();
            String[] lparts = line.split("\t");
            if (lparts.length >= 2) {
                context.write(new Text(lparts[1]), new Text(lparts[0]));
            } else {
                context.write(new Text(""), new Text("Error processing" + lineText));
            }
        }

    }

    /*
        Reducer will simply write to contex
     */
    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text word, Iterable<Text> counts, Context context) throws IOException, InterruptedException {
            List<String> datas = new ArrayList<>();
            for (Text count : counts) {
                if (!datas.contains(count.toString())) {
                    context.write(word, count);
                    datas.add(count.toString());
                }

            }
        }
    }
}
