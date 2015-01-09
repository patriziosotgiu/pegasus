/**
 * PIQASUS: Connected-component analysis for Big Graph
 *
 * __________.___________      _____    _____________ ___  _________
 * \______   \   \_____  \    /  _  \  /   _____/    |   \/   _____/
 *  |     ___/   |/  / \  \  /  /_\  \ \_____  \|    |   /\_____  \
 *  |    |   |   /   \_/.  \/    |    \/        \    |  / /        \
 *  |____|   |___\_____\ \_/\____|__  /_______  /______/ /_______  /
 *                      \__>        \/        \/                 \/
 *
 * Copyright (c) 2014 PlaceIQ, Inc
 *
 * This software is licensed under Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * ----------------------------------------------------------------------------
 * Author: Jerome Serrano <jerome.serrano@placeiq.com>
 * Date: 2015-01-09
 * ---------------------------------------------------------------------------*/

package com.placeiq.piqasus;

import gnu.trove.list.array.TLongArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

//
// IterationStage2: group blocks by row and compute the +
//
public class IterationStage2 {

    public static class _Reducer extends Reducer<VLongWritable, BlockWritable, BlockIndexWritable, BlockWritable> {
        private final BlockIndexWritable KEY   = new BlockIndexWritable();
        private final BlockWritable      VALUE = new BlockWritable();

        private int blockSize;
        private TLongArrayList res = null;
        private BlockWritable initialVector = new BlockWritable();

        @Override
        public void setup(Reducer.Context ctx) {
            Configuration conf = ctx.getConfiguration();
            blockSize = conf.getInt(Constants.PROP_BLOCK_SIZE, 32);
            res = new TLongArrayList(blockSize);
        }

        @Override
        public void reduce(VLongWritable key, Iterable<BlockWritable> values, Context ctx) throws IOException, InterruptedException {
            boolean gotInitialVector = false;
            boolean isInitialVector = true;
            int n = 0;

            res.fill(0, blockSize, -2);
            for (BlockWritable block : values) {
                //System.out.println("_Reducer.reduce input: " + key + "," + block);
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
                ctx.getCounter(PiqasusCounter.ERROR_MISSING_SELF_VECTOR).increment(1);
                System.err.println("ERROR: self_vector == null, key=" + key + ", # values" + n);
                return;
            }

            boolean noChange = initialVector.getVectorElemValues().equals(res);
            BlockWritable.TYPE type = (noChange) ? BlockWritable.TYPE.VECTOR_FINAL : BlockWritable.TYPE.VECTOR_INCOMPLETE;
            VALUE.setVector(type, res);
            KEY.setVectorIndex(key.get());
            ctx.write(KEY, VALUE);
            //System.out.println("_Reducer.reduce: " + KEY + "," + VALUE);
            ctx.getCounter(noChange ? PiqasusCounter.NUMBER_FINAL_VECTOR : PiqasusCounter.NUMBER_INCOMPLETE_VECTOR)
                    .increment(1);
        }
    }
}
