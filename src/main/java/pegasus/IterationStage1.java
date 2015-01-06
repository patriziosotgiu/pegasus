/***********************************************************************
 PEGASUS: Peta-Scale Graph Mining System
 Copyright (C) 2010 U Kang, Duen Horng Chau, and Christos Faloutsos
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

//
// IterationStage1: group blocks by (matrix_column | vector_row) and compute the *
//
// Use secondary sorting so that values in the reducers are sorted by types: vector block then matrix blocks.
//
public class IterationStage1 {

    public static class GIMV {
        private final static long NO_VALUE = -1L;

        public static TLongArrayList minBlockVector(TIntArrayList matrixIndexes,
                                                    TLongArrayList vectorValues)
        {
            TLongArrayList output = new TLongArrayList(vectorValues.size());
            output.fill(0, vectorValues.size(), NO_VALUE);
            int max = matrixIndexes.size() / 2;
            for (int i = 0; i < max; i++) {
                int matrixElementRow = matrixIndexes.getQuick(2 * i);
                int matrixElementColumn = matrixIndexes.getQuick(2 * i + 1);
                long val = vectorValues.getQuick(matrixElementColumn);
                long currentVal = output.getQuick(matrixElementRow);
                if (val != NO_VALUE && (val < currentVal || currentVal == NO_VALUE)) {
                    output.setQuick(matrixElementRow, val);
                }
            }
            return output;
        }

        public static TLongArrayList minBlockVector(BlockWritable block, BlockWritable vect) {
            return minBlockVector(block.getMatrixElemIndexes(), vect.getVectorElemValues());
        }
    };

    public static class JoinKey implements WritableComparable<JoinKey> {
        private boolean isVector;
        private long index;

        public JoinKey(boolean isVector, long index) {
            this.isVector = isVector;
            this.index = index;
        }

        public JoinKey() {
            this.isVector = false;
            this.index = -1;
        }

        @Override
        public int compareTo(JoinKey o) {
            int cmp = Long.compare(index, o.index);
            if (cmp != 0) {
                return cmp;
            }
            return - Boolean.compare(isVector, o.isVector);
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            if (isVector) {
                WritableUtils.writeVLong(dataOutput, - (index + 1));
            } else {
                WritableUtils.writeVLong(dataOutput, index + 1);
            }
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            long v = WritableUtils.readVLong(dataInput);
            if (v < 0) {
                isVector = true;
                index = -v - 1;
            } else {
                isVector = false;
                index = v - 1;
            }
        }

        public void set(boolean isVector, long index) {
            this.isVector = isVector;
            this.index = index;
        }

        public long getIndex() {
            return this.index;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("isVector", isVector)
                    .add("index", index)
                    .toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            JoinKey that = (JoinKey) o;

            if (index != that.index) return false;
            if (isVector != that.isVector) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = (isVector ? 1 : 0);
            result = 31 * result + (int) (index ^ (index >>> 32));
            return result;
        }
    }

    public static class IndexComparator extends WritableComparator {

        protected IndexComparator() {
            super(JoinKey.class, true);
        }

        @Override
        public int compare(WritableComparable o1, WritableComparable o2) {
            JoinKey k1 = (JoinKey)o1;
            JoinKey k2 = (JoinKey)o2;
            return Long.compare(k1.index, k2.index);
        }
    }

    public static class IndexPartitioner extends Partitioner<JoinKey, BlockWritable> {
        @Override
        public int getPartition(JoinKey key, BlockWritable value, int numReduceTasks) {
            long index = key.getIndex();
            int hashCode = 31 * (int) (index ^ (index >>> 32));
            return (hashCode & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    public static class SortComparator extends WritableComparator {
        protected SortComparator() {
            super(JoinKey.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
            long v1;
            long v2;
            try {
                v1 = readVLong(b1, s1);
                v2 = readVLong(b2, s2);
            } catch (IOException e) {
                throw new RuntimeException("corrupted data, failed to parse JoinKey");
            }
            int cmp = Long.compare(Math.abs(v1), Math.abs(v2));
            if (cmp != 0) {
                return cmp;
            }
            boolean isVector1 = v1 < 0;
            boolean isVector2 = v2 < 0;
            cmp = - Boolean.compare(isVector1, isVector2);
            return cmp;
        }
    }

    // TODO: use 2 distinct mappers and multiple input to avoid the if-else condition
    // output negative key to identify
    public static class _Mapper extends Mapper<BlockIndexWritable, BlockWritable, JoinKey, BlockWritable> {

        private final JoinKey KEY = new JoinKey();
        private BlockWritable VALUE;

        @Override
        public void setup(Context ctx) {
            Configuration conf = ctx.getConfiguration();
            VALUE = new BlockWritable(conf.getInt("blockWidth", 32));
        }

        @Override
        public void map(BlockIndexWritable key, BlockWritable value, Context ctx) throws IOException, InterruptedException {
            if (value.isTypeVector()) {
                VALUE.set(value);
                KEY.set(true, key.getI());
                ctx.getCounter(PegasusCounter.NUMBER_BLOCK_VECTOR).increment(1);
            }
            else {
                KEY.set(false, key.getJ());
                VALUE.set(value);
                VALUE.setBlockRow(key.getI());
                ctx.getCounter(PegasusCounter.NUMBER_BLOCK_MATRIX).increment(1);
            }
            ctx.write(KEY, VALUE);
            //System.out.println("_Mapper.map: " + KEY + ", " + VALUE);
        }
    }

    public static class _Reducer extends Reducer<JoinKey, BlockWritable, VLongWritable, BlockWritable> {

        private final BlockWritable INITIAL_VECTOR = new BlockWritable();
        private final VLongWritable KEY            = new VLongWritable();
        private final BlockWritable VALUE          = new BlockWritable();

        @Override
        public void reduce(JoinKey key, Iterable<BlockWritable> values, Context ctx) throws IOException, InterruptedException {

            Iterator<BlockWritable> it = values.iterator();
            INITIAL_VECTOR.set(it.next());
            //System.out.println("_Reducer.reduce input value: " + key + "," + INITIAL_VECTOR);

            if (!INITIAL_VECTOR.isTypeVector()) {
                // missing vector... should never happen, right ? throw exception ?
                ctx.getCounter(PegasusCounter.ERROR_NO_INITIAL_VECTOR).increment(1);
                System.err.println("error: no vector, key=" + key + ", first_value=" + INITIAL_VECTOR);
                return;
            }

            VALUE.set(BlockWritable.TYPE.VECTOR_INITIAL, INITIAL_VECTOR);
            KEY.set(key.index);
            ctx.write(KEY, VALUE);
            //System.out.println("_Reducer.reduce: " + KEY + "," + VALUE);

            while (it.hasNext()) {
                BlockWritable e = it.next();
                //System.out.println("_Reducer.reduce input value: " + key + "," + e + ", initial vector: " + INITIAL_VECTOR);
                KEY.set(e.getBlockRow());
                VALUE.setVector(BlockWritable.TYPE.VECTOR_INCOMPLETE, GIMV.minBlockVector(e, INITIAL_VECTOR));
                ctx.write(KEY, VALUE);
                //System.out.println("_Reducer.reduce: " + KEY + "," + VALUE);
            }
        }
    }
}
