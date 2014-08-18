package pegasus;


import com.google.common.base.Objects;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class ElemArrayWritable implements Writable {

    private ArrayList<VectorElem> vectors = new ArrayList<VectorElem>();
    private ArrayList<BlockElem> blocks = new ArrayList<BlockElem>();
    private long blockCol = -1;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(vectors.size());
        for (VectorElem e : vectors) {
            dataOutput.writeShort(e.row);
            dataOutput.writeLong(e.val);
        }
        dataOutput.writeInt(blocks.size());
        for (BlockElem e : blocks) {
            dataOutput.writeShort(e.row);
            dataOutput.writeShort(e.col);
            dataOutput.writeLong(e.val);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int n = dataInput.readInt();
        vectors.clear();
        vectors.ensureCapacity(n);
        for (int i = 0; i < n; i++) {
            short row = dataInput.readShort();
            long value = dataInput.readLong();
            VectorElem e = new VectorElem(row, value);
            vectors.add(i, e);
        }
        n = dataInput.readInt();
        blocks.clear();
        blocks.ensureCapacity(n);
        for (int i = 0; i < n; i++) {
            short row = dataInput.readShort();
            short col = dataInput.readShort();
            long value = dataInput.readLong();
            BlockElem e = new BlockElem(row, col, value);
            blocks.add(i, e);
        }
    }

    public void reset() {
        vectors.clear();
        blocks.clear();
    }

    public void addVector(short row, long value) {
        vectors.add(new VectorElem(row, value));
    }

    public void addBlock(short row, short col, long value) {
        blocks.add(new BlockElem(row, col, value));
    }

    public ArrayList<VectorElem> getVectors() {
        return vectors;
    }

    public ArrayList<BlockElem> getBlocks() {
        return blocks;
    }

    public void setBlockCol(long c) {
        blockCol = c;
    }

    public long getBlockCol() {
        return blockCol;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("vectors", vectors)
                .add("blocks", blocks)
                .add("blockCol", blockCol)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ElemArrayWritable that = (ElemArrayWritable) o;

        if (blockCol != that.blockCol) return false;
        if (blocks != null ? !blocks.equals(that.blocks) : that.blocks != null) return false;
        if (vectors != null ? !vectors.equals(that.vectors) : that.vectors != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = vectors != null ? vectors.hashCode() : 0;
        result = 31 * result + (blocks != null ? blocks.hashCode() : 0);
        result = 31 * result + (int) (blockCol ^ (blockCol >>> 32));
        return result;
    }
}
