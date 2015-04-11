package garbagetown;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by garbagetown on 4/6/15.
 */
public class MyMapper extends Mapper<LongWritable, Text, CompositeKey, Text> {

    private static Logger logger = LogManager.getLogger(MyMapper.class);

    int index;
    List<Integer> attributeIndexes = new ArrayList<>();
    CompositeKey compositeKey = new CompositeKey();
    List<String> mapValues = new ArrayList<>();
    Text mapValue = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String name = fileSplit.getPath().getName();

        index = Integer.parseInt(context.getConfiguration().get(name));
        if (index == MyDriver.INDEX_E) {
            attributeIndexes.add(2); // FName
            attributeIndexes.add(3); // LName
            attributeIndexes.add(4); // Gender
            attributeIndexes.add(6); // DeptNo
        } else {
            attributeIndexes.add(1); // Salary
            attributeIndexes.add(3); // Effective-to-date
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        if (value.toString().length() <= 0) {
            logger.error(value.toString());
            return;
        }

        String[] tokens = value.toString().split(",");

        compositeKey.setKey(tokens[0]);
        compositeKey.setIndex(index);

        mapValue.clear();
        mapValues.clear();
        for (Integer index : attributeIndexes) {
            mapValues.add(tokens[index]);
        }
        mapValue.set(StringUtils.join(mapValues, ","));

        context.write(compositeKey, mapValue);
    }
}
