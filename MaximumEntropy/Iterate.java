package wordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

/**
 * @author Devdatta
 */
public class Iterate extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(Iterate.class);
    int totalTweets;

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), " init ");
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Iterate.Map.class);
        job.setReducerClass(Iterate.Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.getConfiguration().setInt("totalTweets", totalTweets);
        job.setOutputValueClass(Text.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /*
        Edited mapper to emit filename along with word. Reducer not thanged
     */
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

        public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
            String line = lineText.toString();
            String[] splitted;
            String[] splittedRec;
            String tweetId = line.split("\t")[0];
            line = line.split("\t")[1];
            splitted = line.split(Main.RECORD_SEP);
            double f, l;
            List<IntermediateData> datas = new ArrayList<>();
            for (int i = 0; i < splitted.length; i++) {
                splittedRec = splitted[i].split(Main.FIELD_SEP);
                IntermediateData id = new IntermediateData();
                id.setWord(splittedRec[0]);
                id.setTweetClass(splittedRec[1]);
                id.setTweetId(tweetId);
                id.setF(Double.parseDouble(splittedRec[2]));
                id.setL(Double.parseDouble(splittedRec[3]));
                datas.add(id);
            }

            double[] M = {0.0};
            datas.stream().map(d -> d.getF())
                    .max((Double o1, Double o2) -> o1.compareTo(o2))
                    .ifPresent(val -> M[0] = val);

//            double delta = (1/M)*(Math.log())
            double lamfbaFi = datas.stream().
                    mapToDouble(d -> d.getF() * d.getL())
                    .sum();
            double pLambda = Math.exp(lamfbaFi) / Math.exp(lamfbaFi + 2);

            double numerator = datas.stream().mapToDouble(d -> d.getF()).sum();
            double denominator = datas.stream().mapToDouble(d -> d.getF() * pLambda).sum();
            double delta = numerator / denominator;

            datas.stream().forEach(data -> {
                try {
                    context.write(new Text(data.getTweetId()), new Text(data.getWord() + Main.FIELD_SEP + data.getTweetClass() + Main.FIELD_SEP + data.getF() + Main.FIELD_SEP + (data.getL() + delta)));
                } catch (IOException ex) {
                    throw new RuntimeException("exception", ex);
                } catch (InterruptedException ex) {
                    throw new RuntimeException("exception", ex);
                }
            });
        }
    }

    /*
        Reducer will simply count and write to context
     */
    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text word, Iterable<Text> counts, Context context) throws IOException, InterruptedException {
            StringBuffer buffer = new StringBuffer();
            String prefix = "";
            for (Text count : counts) {
                buffer.append(prefix);
                prefix = Main.RECORD_SEP;
                buffer.append(count.toString());
                //total docs
                context.getCounter("totalCount", "cnt").increment(1);
            }
            context.write(word, new Text(buffer.toString()));
        }
    }
}
