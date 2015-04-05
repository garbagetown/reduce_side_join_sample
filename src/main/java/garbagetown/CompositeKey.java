package garbagetown;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by garbagetown on 4/6/15.
 */
public class CompositeKey implements WritableComparable<CompositeKey> {

    private String key;
    private int index;

    public CompositeKey(String key, int index) {
        this.key = key;
        this.index = index;
    }

    @Override
    public int compareTo(CompositeKey o) {
        int result = key.compareTo(o.key);
        if (result == 0) {
            result = Integer.compare(index, o.index);
        }
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, key);
        WritableUtils.writeVInt(out, index);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        key = WritableUtils.readString(in);
        index = WritableUtils.readVInt(in);
    }

    @Override
    public String toString() {
        return String.format("%s\t%s", key, index);
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }
}
