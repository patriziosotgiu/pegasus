package pegasus;

import com.google.common.base.Objects;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;


public class VectorElemWritable implements Writable {

    public static enum TYPE {
        NONE(0), MSI(1), MSF(2), MOI(3);
        private final short value;
        private TYPE(int v) { value = (short)v; }
        private short getValue() { return value; }
        public static TYPE get(int code) {
            switch(code) {
                case 0: return NONE;
                case 1: return MSI;
                case 2: return MSF;
                case 3: return MOI;
            }
            return null;
        }
    };

    private TYPE type = TYPE.NONE;
    private ArrayList<VectorElem> vectors = new ArrayList<VectorElem>();

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeShort(type.getValue());
        dataOutput.writeInt(vectors.size());
        for (VectorElem e : vectors) {
            dataOutput.writeShort(e.row);
            dataOutput.writeLong(e.val);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        type = TYPE.get(dataInput.readShort());
        int n = dataInput.readInt();
        vectors.clear();
        vectors.ensureCapacity(n);
        for (int i = 0; i < n; i++) {
            short row = dataInput.readShort();
            long value = dataInput.readLong();
            VectorElem e = new VectorElem(row, value);
            vectors.add(i, e);
        }
    }

    public void set(TYPE t, ArrayList<VectorElem> v) {
        type = t;
        vectors.clear();
        vectors.addAll(v);
    }

    public TYPE getType() {
        return type;
    }

    public ArrayList<VectorElem> getVector() {
        return vectors;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("type", type)
                .add("vectors", vectors)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VectorElemWritable that = (VectorElemWritable) o;

        if (type != that.type) return false;
        if (!vectors.equals(that.vectors)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + vectors.hashCode();
        return result;
    }
}
