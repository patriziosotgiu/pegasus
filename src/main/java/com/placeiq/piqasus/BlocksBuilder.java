/**
 * PIQASUS: Connected-component analysis for Big Graph
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
 */

package com.placeiq.piqasus;

import com.google.common.base.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BlocksBuilder extends Configured implements Tool {

    public static class LightBlockWritable implements Writable {

        private int  index1 = -1;
        private int  index2 = -1;
        private long value  = -1;

        public LightBlockWritable() {
        }

        @Override
        public void write(DataOutput out) throws IOException {
            if (value != -1) {
                WritableUtils.writeVInt(out, - (index1 + 1));
                WritableUtils.writeVLong(out, value);
            }
            else {
                WritableUtils.writeVInt(out, index1 + 1);
                WritableUtils.writeVInt(out, index2);
                WritableUtils.writeVLong(out, value);
            }
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            int v = WritableUtils.readVInt(in);
            if (v < 0) { // vector
                this.index1 = -v - 1;
                this.index2 = -1;
                this.value = WritableUtils.readVLong(in);
            }
            else { // matrix
                this.index1 = v - 1;
                this.index2 = WritableUtils.readVInt(in);
                this.value = -1;
            }
        }

        public void setVector(int idx, long value) {
            this.index1 = idx;
            this.index2 = -1;
            this.value = value;
        }

        public void setMatrix(int idxRow, int idxColumn) {
            this.index1 = idxRow;
            this.index2 = idxColumn;
            this.value = -1;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            LightBlockWritable that = (LightBlockWritable) o;

            if (index1 != that.index1) return false;
            if (index2 != that.index2) return false;
            if (value != that.value) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = index1;
            result = 31 * result + index2;
            result = 31 * result + (int) (value ^ (value >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("index1", index1)
                    .add("index2", index2)
                    .add("value", value)
                    .toString();
        }
    }


    public static class MapStage1 extends Mapper<LongWritable, Text, BlockIndexWritable, LightBlockWritable> {

        private final BlockIndexWritable KEY   = new BlockIndexWritable();
        private final LightBlockWritable VALUE = new LightBlockWritable();

        private int     blockSize;
        private boolean isVector;

        @Override
        public void setup(Context ctx) {
            Configuration conf = ctx.getConfiguration();
            blockSize = conf.getInt("blockWidth", 32);
            isVector = conf.getBoolean("isVector", false);
        }

        @Override
        public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");

            if (isVector) {
                long rowIdx     = Long.parseLong(line[0]);
                long blockIdx   = rowIdx / blockSize;
                int  inBlockIdx = (int) (rowIdx % blockSize);

                VALUE.setVector(inBlockIdx, Long.parseLong(line[1]));
                KEY.setVectorIndex(blockIdx);
                ctx.write(KEY, VALUE);
            } else {
                long rowIdx = Long.parseLong(line[0]);
                long colIdx = Long.parseLong(line[1]);

                if (colIdx == rowIdx) {
                    ctx.getCounter(PiqasusCounter.NUMBER_SELF_LOOP).increment(1);
                    return;
                }

                long blockRowIdx   = rowIdx / blockSize;
                long blockColIdx   = colIdx / blockSize;
                int  inBlockRowIdx = (int) (rowIdx % blockSize);
                int  inBlockColIdx = (int) (colIdx % blockSize);

                VALUE.setMatrix(inBlockRowIdx, inBlockColIdx);
                KEY.setMatrixIndex(blockRowIdx, blockColIdx);
                ctx.write(KEY, VALUE);

                VALUE.setMatrix(inBlockColIdx, inBlockRowIdx);
                KEY.setMatrixIndex(blockColIdx, blockRowIdx);
                ctx.write(KEY, VALUE);
            }
        }
    }

    public static class RedStage1 extends Reducer<BlockIndexWritable, LightBlockWritable, BlockIndexWritable, BlockWritable> {
        private BlockWritable VALUE      = null;
        private int           blockSize  = 32;
        private boolean       isVector   = false;

        @Override
        public void setup(Context ctx) {
            Configuration conf = ctx.getConfiguration();
            blockSize = conf.getInt("blockWidth", 32);
            isVector = conf.getBoolean("isVector", false);
            VALUE = new BlockWritable(blockSize, isVector ? BlockWritable.TYPE.VECTOR_INITIAL : BlockWritable.TYPE.MATRIX);
        }

        @Override
        public void reduce(BlockIndexWritable key, Iterable<LightBlockWritable> values, Context ctx) throws IOException, InterruptedException {
            if (isVector) {
                VALUE.resetVector();
                boolean initVector = false;
                for (LightBlockWritable block : values) {
                    if (!initVector) {
                        VALUE.setVectorInitialValue(blockSize);
                        initVector = true;
                    }
                    VALUE.setVectorElem(block.index1, block.value);
                }
                ctx.write(key, VALUE);
            }
            else {
                VALUE.resetMatrix();
                for (LightBlockWritable block : values) {
                    VALUE.addMatrixElem(block.index1, block.index2);
                }
                ctx.write(key, VALUE);
            }
        }
    }

    private Path    pathEdges        = null;
    private Path    pathOutput       = null;
    private int     blockSize        = 1;
    private int     numberOfReducers = 1;
    private boolean isVector         = false;

    public static void main(final String[] args) throws Exception {
        final int result = ToolRunner.run(new Configuration(), new BlocksBuilder(), args);
        System.exit(result);
    }

    public int run(final String[] args) throws Exception {
        pathEdges        = new Path(args[0]);
        pathOutput       = new Path(args[1]);
        blockSize        = Integer.parseInt(args[2]);
        numberOfReducers = Integer.parseInt(args[3]);
        isVector         = args[4].equals("vector");

        if (!configStage1().waitForCompletion(true)) {
            System.err.println("Failed to execute BlocksBuilder");
            return -1;
        }
        return 0;
    }

    protected Job configStage1() throws Exception {
        FileSystem fs = FileSystem.get(getConf());
        fs.delete(pathOutput, true);   // useful ?

        Configuration conf = getConf();
        conf.setInt("blockSize", blockSize);
        conf.setBoolean("isVector", isVector);
        conf.set("mapred.output.compression.type", "BLOCK"); // useful ?

        Job job = new Job(conf, "data-piqid.piqasus.BlocksBuilder");
        job.setJarByClass(BlocksBuilder.class);
        job.setMapperClass(MapStage1.class);
        job.setReducerClass(RedStage1.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setNumReduceTasks(numberOfReducers);
        job.setMapOutputKeyClass(BlockIndexWritable.class);
        job.setMapOutputValueClass(LightBlockWritable.class);
        job.setOutputKeyClass(BlockIndexWritable.class);
        job.setOutputValueClass(BlockWritable.class);

        FileInputFormat.setInputPaths(job, pathEdges);
        SequenceFileOutputFormat.setOutputPath(job, pathOutput);
        SequenceFileOutputFormat.setCompressOutput(job, true);

        Runner.setCompression(job);

        return job;
    }
}