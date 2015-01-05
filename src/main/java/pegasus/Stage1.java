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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

//
// Stage1: group blocks by (matrix_column | vector_row) and compute the *
//
// Use secondary sorting so that values in the reducers are sorted by types: vector block then matrix blocks.
//
public class Stage1 {

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

    public static class IndexPartitioner implements Partitioner<JoinKey, BlockWritable> {

        public void configure(JobConf job) {}

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
    public static class Mapper1 extends MapReduceBase implements Mapper<BlockIndexWritable, BlockWritable, JoinKey, BlockWritable> {

        private static JoinKey KEY   = new JoinKey();
        private static BlockWritable VALUE;

        public void configure(JobConf job) {
            int block_width = Integer.parseInt(job.get("block_width"));
            VALUE = new BlockWritable(block_width);
        }

        public void map(final BlockIndexWritable key, final BlockWritable value, final OutputCollector<JoinKey, BlockWritable> output, final Reporter reporter) throws IOException {
            if (value.isTypeVector()) {
                VALUE.set(value);
                KEY.set(true, key.getI());
                reporter.incrCounter("PEGASUS", "Number of vector blocks", 1);
            }
            else {
                KEY.set(false, key.getJ());
                VALUE.set(value);
                VALUE.setBlockRow(key.getI());
                reporter.incrCounter("PEGASUS", "Number of matrix blocks", 1);
            }
            output.collect(KEY, VALUE);
            //System.out.println("Mapper1.map: " + KEY + ", " + VALUE);
        }
    }

    public static class Reducer1 extends MapReduceBase implements Reducer<JoinKey, BlockWritable, LongWritable, BlockWritable> {
        protected int blockWidth;

        private BlockWritable initialVector = new BlockWritable();

        private LongWritable  KEY   = new LongWritable();
        private BlockWritable VALUE = new BlockWritable();

        public void configure(JobConf job) {
            blockWidth = Integer.parseInt(job.get("block_width"));
            System.out.println("Reducer1: block_width=" + blockWidth);
        }

        public void reduce(final JoinKey key, final Iterator<BlockWritable> values, OutputCollector<LongWritable, BlockWritable> output, final Reporter reporter) throws IOException {

            initialVector.set(values.next());
            //System.out.println("Reducer1.reduce input value: " + key + "," + initialVector);

            if (!initialVector.isTypeVector()) {
                // missing vector... should never happen, right ? throw exception ?
                reporter.incrCounter("ERROR", "no_vector", 1);
                System.err.println("error: no vector, key=" + key + ", first_value=" + initialVector);
                return;
            }

            VALUE.set(BlockWritable.TYPE.VECTOR_INITIAL, initialVector);
            KEY.set(key.index);
            output.collect(KEY, VALUE);
            //System.out.println("Reducer1.reduce: " + KEY + "," + VALUE);

            while (values.hasNext()) {
                BlockWritable e = values.next();
                //System.out.println("Reducer1.reduce input value: " + key + "," + e + ", initial vector: " + initialVector);
                KEY.set(e.getBlockRow());
                VALUE.setVector(BlockWritable.TYPE.VECTOR_INCOMPLETE, GIMV.minBlockVector(e, initialVector));
                output.collect(KEY, VALUE);
                //System.out.println("Reducer1.reduce: " + KEY + "," + VALUE);
            }
        }
    }
}
