package garbagetown;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by garbagetown on 4/6/15.
 */
public class GroupingComparator extends WritableComparator {

    protected GroupingComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CompositeKey key1 = (CompositeKey) a;
        CompositeKey key2 = (CompositeKey) b;
        return key1.getKey().compareTo(key2.getKey());
    }
}
