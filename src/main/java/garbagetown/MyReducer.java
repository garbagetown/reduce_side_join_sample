package garbagetown;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by garbagetown on 4/6/15.
 */
public class MyReducer extends Reducer<CompositeKey, Text, NullWritable, Text> {

    private static Logger logger = LogManager.getLogger(MyReducer.class);

    MapFile.Reader reader;
    Text deptCode = new Text();
    Text deptName = new Text();
    List<String> reduceValues = new ArrayList<>();
    Text reduceOutputValue = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] paths = context.getCacheFiles();
        if (paths.length != 1) {
            logger.error(paths);
        }
        reader = new MapFile.Reader(new Path(paths[0]), context.getConfiguration());
    }

    @Override
    protected void reduce(CompositeKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Iterator<Text> iterator = values.iterator();

        Text employee = iterator.next();

        deptCode.set(employee.toString().split(",")[3]);
        reader.get(deptCode, deptName);

        String salary = StringUtils.EMPTY;
        while (iterator.hasNext()) {
            String[] tokens = iterator.next().toString().split(",");
            if (tokens.length == 2 && StringUtils.equals(tokens[1], "9999-01-01")) {
                salary = tokens[0];
            }
        }

        reduceValues.clear();
        reduceValues.add(employee.toString());
        reduceValues.add(deptName.toString());
        reduceValues.add(salary);

        reduceOutputValue.set(StringUtils.join(reduceValues, ","));

        logger.info("==============================");
        logger.info(employee);
        logger.info(deptName);
        logger.info(salary);
        logger.info(reduceOutputValue);
        logger.info("==============================");

        context.write(NullWritable.get(), reduceOutputValue);
    }
}
