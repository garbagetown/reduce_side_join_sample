package garbagetown;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.net.URI;

/**
 * Created by garbagetown on 4/6/15.
 */
public class MyDriver extends Configured implements Tool {

    private static Logger logger = LogManager.getLogger(MyDriver.class);

    public static final String NAME_E = "part-e";
    public static final String NAME_SC = "part-sc";
    public static final String NAME_SH = "part-sh";

    public static final int INDEX_E = 1;
    public static final int INDEX_SC = 2;
    public static final int INDEX_SH = 3;

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 3) {
            return -1;
        }

        Job job = Job.getInstance();
        Configuration conf = job.getConfiguration();
        job.setJarByClass(MyDriver.class);
        job.setJobName("reduce_side_join_sample");

        job.addCacheFile(new URI("input/department.txt"));

        conf.setInt(NAME_E, INDEX_E); // Set Employee file to 1
        conf.setInt(NAME_SC, INDEX_SC); // Set Current salary file to 2
        conf.setInt(NAME_SH, INDEX_SH); // Set Historical salary file to 3

        FileInputFormat.setInputPaths(job, new Path(args[0]), new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(4);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        boolean success = job.waitForCompletion(true);

        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new MyDriver(), args);
        System.exit(exitCode);
    }
}
