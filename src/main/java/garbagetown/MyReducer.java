package garbagetown;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

/**
 * Created by garbagetown on 4/6/15.
 */
public class MyReducer extends Reducer<CompositeKey, Text, NullWritable, Text> {

    private static Logger logger = LogManager.getLogger(MyReducer.class);

    Text deptCode = new Text();
    Text deptName = new Text();
    Map<String, String> departmentMap = new HashMap<>();
    List<String> reduceValues = new ArrayList<>();
    Text reduceOutputValue = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] paths = context.getCacheFiles();
        if (paths.length != 1) {
            logger.error(paths);
        }
        Path path = new Path(paths[0]);
        FileSystem fs = FileSystem.get(new Configuration());
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
            String line = reader.readLine();
            while (line != null) {
                String[] tokens = line.split(",");
                departmentMap.put(tokens[0], tokens[1]);
                line = reader.readLine();
            }
        }
    }

    @Override
    protected void reduce(CompositeKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


        Iterator<Text> iterator = values.iterator();

        String employee = iterator.next().toString();

        deptCode.set(employee.split(",")[3]);
        deptName.set(departmentMap.get(deptCode.toString()));

        String salary = StringUtils.EMPTY;
        while (iterator.hasNext()) {
            String[] tokens = iterator.next().toString().split(",");
            logger.info(Arrays.asList(tokens));
            if (tokens.length == 2 && StringUtils.equals(tokens[1], "9999-01-01")) {
                salary = tokens[0];
            }
        }

        reduceValues.clear();
        reduceValues.add(employee.toString());
        reduceValues.add(deptName.toString());
        reduceValues.add(salary);

        reduceOutputValue.set(StringUtils.join(reduceValues, ","));

        context.write(NullWritable.get(), reduceOutputValue);
    }
}
