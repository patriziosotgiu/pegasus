package pegasus;


import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableComparator;

import java.io.IOException;

public class VLongWritableComparator extends WritableComparator {
    public VLongWritableComparator() {
        super(VLongWritable.class);
    }

    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
        long thisValue = 0;
        long thatValue = 0;
        try {
            thisValue = readVLong(b1, s1);
            thatValue = readVLong(b2, s2);
        } catch (IOException e) {
            throw new RuntimeException("corrupted data, failed to parse VLongWritable");
        }
        return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
    }
}