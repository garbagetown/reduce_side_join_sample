package garbagetown;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by garbagetown on 4/6/15.
 */
public class SortComparator extends WritableComparator {

    protected SortComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        CompositeKey key1 = (CompositeKey) a;
        CompositeKey key2 = (CompositeKey) b;

        int result = key1.getKey().compareTo(key2.getKey());
        if (result == 0) {
            result = Integer.compare(key1.getIndex(), key2.getIndex());
        }
        return result;
    }
}
