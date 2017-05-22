package TopRecord;

/**
 * Created by Onerepublic on 5/5/17.
 */
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TopKComparator extends WritableComparator {
    protected TopKComparator() {
        super(DoubleWritable.class, true);
    }

    public int compare(WritableComparable w1, WritableComparable w2) {
        DoubleWritable key1 = (DoubleWritable)w1;
        DoubleWritable key2 = (DoubleWritable)w2;
        return -1 * key1.compareTo(key2);
    }
}
