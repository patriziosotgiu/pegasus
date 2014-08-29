package pegasus;

import com.google.common.base.Objects;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.list.array.TShortArrayList;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BlockWritable implements Writable {

    public static enum TYPE {
        NONE(0), INITIAL(1), FINAL(2), INCOMPLETE(3);
        private final short value;
        private TYPE(int v) { value = (short)v; }
        private short getValue() { return value; }
        public static TYPE get(int code) {
            switch(code) {
                case 0: return NONE;
                case 1: return INITIAL;
                case 2: return FINAL;
                case 3: return INCOMPLETE;
            }
            return null;
        }
    };

    private TYPE type = TYPE.NONE;

    private boolean isVector = true;

    // Only for matrix block
    private final TShortArrayList matrixElemIndexes = new TShortArrayList();  // (col, rows)
    private long blockRow;


    // Only for vector block
    private final TLongArrayList vectorElemValues = new TLongArrayList();

    public BlockWritable(BlockWritable b) {
        set(b);
    }

    public BlockWritable() {

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBoolean(isVector);
        dataOutput.writeShort(type.getValue());
        if (isVector) {
            dataOutput.writeInt(vectorElemValues.size());
            for (int i = 0; i < vectorElemValues.size(); i++) {
                dataOutput.writeLong(vectorElemValues.get(i));
            }
        }
        else {
            dataOutput.writeLong(blockRow);
            dataOutput.writeInt(matrixElemIndexes.size());
            for (int i = 0; i < matrixElemIndexes.size(); i++) {
                dataOutput.writeShort(matrixElemIndexes.get(i));
            }
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        reset();
        isVector = dataInput.readBoolean();
        type = TYPE.get(dataInput.readShort());
        if (isVector) {
            int n = dataInput.readInt();
            for (int i = 0; i < n; i++) {
                vectorElemValues.add(dataInput.readLong());
            }
        }
        else {
            blockRow = dataInput.readLong();
            int n = dataInput.readInt();
            for (int i = 0; i < n; i++) {
                matrixElemIndexes.add(dataInput.readShort());
            }
        }
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("isVector", isVector)
                .add("type", type)
                .add("blockRow", blockRow)
                .add("matrixElemIndexes", matrixElemIndexes)
                .add("vectorElemValues", vectorElemValues)
                .toString();
    }

    public void setVectorElem(short i, long l) {
        vectorElemValues.set(i, l);
    }

    public void setVectorElem(int i, long l) {
        setVectorElem((short) i, l);
    }

    public void addMatrixElem(int i, int j) {
        addMatrixElem((short) i, (short) j);
    }

    public void addMatrixElem(short i, short j) {
        matrixElemIndexes.add(i);
        matrixElemIndexes.add(j);
    } 
    public void reset() {
        vectorElemValues.reset();
        matrixElemIndexes.reset();
        type = TYPE.NONE;
    }

    public void setTypeVector(int n) {
        isVector = true;
        vectorElemValues.ensureCapacity(n);
        vectorElemValues.fill(0, n, -1);
    }

    public void setTypeMatrix() {
        isVector = false;
    }

    public boolean isTypeVector() {
        return isVector;
    }

    public TShortArrayList getMatrixElemIndexes() {
        return matrixElemIndexes;
    }

    public TLongArrayList getVectorElemValues() {
        return vectorElemValues;
    }

    public void setBlockRow(long blockRow) {
        this.blockRow = blockRow;
    }

    public long getBlockRow() {
        return blockRow;
    }

    public void set(TYPE type, BlockWritable b) {
        set(b);
        this.type = type;
    }

    public void set(BlockWritable b) {
        type = b.type;
        isVector = b.isVector;
        matrixElemIndexes.clear();
        vectorElemValues.clear();
        matrixElemIndexes.addAll(b.matrixElemIndexes);
        vectorElemValues.addAll(b.vectorElemValues);
    }

    public void setVector(TYPE type, TLongArrayList values) {
        this.type = type;
        this.isVector = true;
        this.vectorElemValues.clear();
        this.vectorElemValues.addAll(values);
    }

    public TYPE getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BlockWritable that = (BlockWritable) o;

        if (blockRow != that.blockRow) return false;
        if (isVector != that.isVector) return false;
        if (type != that.type) return false;
        if (!matrixElemIndexes.equals(that.matrixElemIndexes)) return false;
        if (!vectorElemValues.equals(that.vectorElemValues)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + (isVector ? 1 : 0);
        result = 31 * result + matrixElemIndexes.hashCode();
        result = 31 * result + vectorElemValues.hashCode();
        result = 31 * result + (int) (blockRow ^ (blockRow >>> 32));
        return result;
    }
}