package garbagetown;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by garbagetown on 4/6/15.
 */
public class MyPartitioner extends Partitioner<CompositeKey, Text> {

    @Override
    public int getPartition(CompositeKey key, Text text, int numPartitions) {
        return key.getKey().hashCode() % numPartitions;
    }
}
