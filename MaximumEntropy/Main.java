package wordcount;

import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author deva
 */
public class Main {

    public static final String RECORD_SEP = "###";
    public static final String FIELD_SEP = "__";
    public static final double ALLOWED_ERROR = 10;

    public static void main(String[] args) throws Exception {
        int totalDocs = ToolRunner.run(new Init(), new String[]{args[0], args[0] + "init"});
        ToolRunner.run(new Iterate(), new String[]{args[0] + "init", args[1]});
        System.exit(1);
    }

}
