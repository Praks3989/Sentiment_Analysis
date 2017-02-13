package wordcount;

import java.io.IOException;
import java.util.TreeSet;
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

/**
 * @author Devdatta
 */
public class Search extends Configured implements Tool {

    private static final String KEYWORDS = "keyWords";

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Search(), args);
        System.exit(res);
    }

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
        job.setNumReduceTasks(1);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /*
        Find sentiment of each word and write it to the context
        As there is single key, single reducer will be auto enforced
     */
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        TreeSet<String> keywords;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String[] k = context.getConfiguration().getStrings(KEYWORDS);
            keywords = new TreeSet<>();
            if (k != null) {
                for (int i = 0; i < k.length; i++) {
                    String string = k[i];
                    keywords.add(string.toLowerCase());
                }
            } else {
                keywords.add("null");
            }
        }

        public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
            String line = lineText.toString();
            //context.write(new Text("----input----"), new Text(line));
            String[] lpatrts = line.split("\t");
            if (keywords.contains(lpatrts[1].toLowerCase())) {
                context.write(new Text("sentiment"), new Text(lpatrts[0]));
            } else {
            }
        }

    }

    /*
        Sum up all sentiments, find average sentiment and classify accordingly
     */
    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int i = 0;
            for (Text value : values) {
                i++;
                try {
                    sum += Double.parseDouble(value.toString());
                } catch (Exception e) {
                    context.write(key, value);
                }
            }
            sum = sum / i;
            Text t = new Text("" + sum);
            context.write(new Text("Average sentiment value"), t);
            if (sum > 1) {
                context.write(new Text("Data is positive"), t);
            }
            if (sum < -1) {
                context.write(new Text("Data is negative"), t);
            }
            if (sum > 1) {
                context.write(new Text("Data is neutral"), t);
            }

        }
    }

}
