/***********************************************************************
 PEGASUS: Peta-Scale Graph Mining System
 Copyright (C) 2014 Jerome Serrano <jerome@placeiq.com>

 This software is licensed under Apache License, Version 2.0 (the  "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 ***********************************************************************/

package pegasus;

import com.google.common.base.Objects;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.list.array.TLongArrayList;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BlockWritable implements Writable {

    public static enum TYPE {
        MATRIX(0), VECTOR_INITIAL(1), VECTOR_FINAL(2), VECTOR_INCOMPLETE(3);
        private final short value;
        private TYPE(int v) { value = (short)v; }
        private short getValue() { return value; }
        public static TYPE get(int code) {
            switch(code) {
                case 0: return MATRIX;
                case 1: return VECTOR_INITIAL;
                case 2: return VECTOR_FINAL;
                case 3: return VECTOR_INCOMPLETE;
            }
            return null;
        }
    };

    private TYPE type = TYPE.MATRIX;

    private TIntArrayList matrixElemIndexes = null;  // (col, rows)
    private long blockRow = -1;

    private TLongArrayList vectorElemValues = null;

    public BlockWritable() {
        System.out.println("Create new BlockWritable with default constructor");
        init(1);
    }

    public BlockWritable(int blockSize, TYPE type) {
        this(blockSize);
        this.type = type;
        if (!TYPE.MATRIX.equals(type)) {
            setVectorInitialValue(blockSize);
        }
    }

    public BlockWritable(int blockSize) {
        System.out.println("Create new BlockWritable with BlockWritable(blocksize)");
        init(blockSize);
    }

    private void init(int blockSize) {
        this.matrixElemIndexes = new TIntArrayList(blockSize);
        this.vectorElemValues = new TLongArrayList(blockSize);
    }

    public void setVectorInitialValue(int blockSize) {
        for (int i = 0; i < blockSize; i++) {
            vectorElemValues.add(-1);
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeByte(type.getValue());
        if (!TYPE.MATRIX.equals(type)) {
            WritableUtils.writeVInt(dataOutput, vectorElemValues.size());
            for (int i = 0; i < vectorElemValues.size(); i++) {
                WritableUtils.writeVLong(dataOutput, vectorElemValues.getQuick(i));
            }
        }
        else {
            WritableUtils.writeVLong(dataOutput, blockRow);
            WritableUtils.writeVInt(dataOutput, matrixElemIndexes.size());
            for (int i = 0; i < matrixElemIndexes.size(); i++) {
                WritableUtils.writeVInt(dataOutput, matrixElemIndexes.get(i));
            }
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        reset();
        type = TYPE.get(dataInput.readByte());
        if (!TYPE.MATRIX.equals(type)) {
            int n = WritableUtils.readVInt(dataInput);
            vectorElemValues.ensureCapacity(n);
            for (int i = 0; i < n; i++) {
                vectorElemValues.add(WritableUtils.readVLong(dataInput));
            }
        }
        else {
            blockRow = WritableUtils.readVLong(dataInput);
            long n = WritableUtils.readVLong(dataInput);
            for (int i = 0; i < n; i++) {
                matrixElemIndexes.add(WritableUtils.readVInt(dataInput));
              //matrixElemIndexes.add(dataInput.readByte());
            }
        }
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("type", type)
                .add("blockRow", blockRow)
                .add("matrixElemIndexes", matrixElemIndexes)
                .add("vectorElemValues", vectorElemValues)
                .toString();
    }

    public void setVectorElem(int i, long l) {
        vectorElemValues.set(i, l);
    }

    public void addMatrixElem(int i, int j) {
        matrixElemIndexes.add(i);
        matrixElemIndexes.add(j);
    }

    public void reset() {
        resetMatrix();
        resetVector();
        blockRow = -1;
    }

    public void resetVector() {
        vectorElemValues.resetQuick();
    }

    public void resetMatrix() {
        matrixElemIndexes.resetQuick();
    }

    public boolean isTypeVector() {
        return !TYPE.MATRIX.equals(type);
    }

    public TIntArrayList getMatrixElemIndexes() {
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
        blockRow = b.blockRow;
        matrixElemIndexes.resetQuick();
        matrixElemIndexes.addAll(b.matrixElemIndexes);
        vectorElemValues.resetQuick();
        vectorElemValues.addAll(b.vectorElemValues);
    }

    public void setVector(TYPE type, TLongArrayList values) {
        this.type = type;
        vectorElemValues.resetQuick();
        vectorElemValues.addAll(values);
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
        if (type != that.type) return false;
        if (!matrixElemIndexes.equals(that.matrixElemIndexes)) return false;
        if (!vectorElemValues.equals(that.vectorElemValues)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + matrixElemIndexes.hashCode();
        result = 31 * result + vectorElemValues.hashCode();
        result = 31 * result + (int) (blockRow ^ (blockRow >>> 32));
        return result;
    }
}