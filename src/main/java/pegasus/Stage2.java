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

import gnu.trove.list.array.TLongArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

//
// Stage2: group blocks by row and compute the +
//
public class Stage2 {

    public static class Reducer2 extends Reducer<VLongWritable, BlockWritable, BlockIndexWritable, BlockWritable> {
        protected int blockWidth;

        private final BlockIndexWritable KEY   = new BlockIndexWritable();
        private final BlockWritable      VALUE = new BlockWritable();

        private TLongArrayList res = null;
        private BlockWritable initialVector = new BlockWritable();

        @Override
        public void setup(Reducer.Context ctx) {
            Configuration conf = ctx.getConfiguration();
            blockWidth = conf.getInt("block_width", 32);
            res = new TLongArrayList(blockWidth);
        }

        @Override
        public void reduce(VLongWritable key, Iterable<BlockWritable> values, Context ctx) throws IOException, InterruptedException {
            boolean gotInitialVector = false;
            res.fill(0, blockWidth, -2);

            int n = 0;
            boolean isInitialVector = true;
            for (BlockWritable block : values) {
                // System.out.println("Reducer2.reduce input: " + key + "," + block);

                BlockWritable.TYPE t = block.getType();
                if (t == BlockWritable.TYPE.VECTOR_FINAL || t == BlockWritable.TYPE.VECTOR_INITIAL) {
                    initialVector.set(block);
                    gotInitialVector = true;
                    isInitialVector = true;
                }
                else {
                    isInitialVector = false;
                }

                for (int i = 0; i < block.getVectorElemValues().size(); i++) {
                    long v = block.getVectorElemValues().getQuick(i);
                    // TODO: not efficient, move isInitialVector check outside the loop
                    // TODO: a bit messy, if block is the initialVector then res will be set to block, usefull ?
                    if (isInitialVector && v == -1L) {
                        res.setQuick(i, -1L);
                    }
                    else if (v != -1L && (res.getQuick(i) == -2 || v < res.getQuick(i))) {
                        res.setQuick(i, v);
                    }
                }
                n++;
            }

            if (!gotInitialVector) {
                ctx.getCounter(PegasusCounter.ERROR_MISSING_SELF_VECTOR).increment(1);
                System.err.println("ERROR: self_vector == null, key=" + key + ", # values" + n);
                return;
            }

            boolean noChange = initialVector.getVectorElemValues().equals(res);
            BlockWritable.TYPE type = (noChange) ? BlockWritable.TYPE.VECTOR_FINAL : BlockWritable.TYPE.VECTOR_INCOMPLETE;
            VALUE.setVector(type, res);
            KEY.setVectorIndex(key.get());
            ctx.write(KEY, VALUE);
            //System.out.println("Reducer2.reduce: " + KEY + "," + VALUE);
            ctx.getCounter(noChange ? PegasusCounter.NUMBER_FINAL_VECTOR : PegasusCounter.NUMBER_INCOMPLETE_VECTOR)
                    .increment(1);
        }
    }
}
