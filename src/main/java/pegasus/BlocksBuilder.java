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

import java.io.*;

import com.google.common.base.Objects;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.*;

public class BlocksBuilder extends Configured implements Tool {

    public static class LightBlockWritable implements Writable {

        private int index1 = -1;
        private int index2 = -1;
        private long value = -1;

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
        private int block_size;
        private boolean isVector;

        private final BlockIndexWritable KEY = new BlockIndexWritable();
        private final LightBlockWritable VALUE = new LightBlockWritable();

        @Override
        public void setup(Context ctx) {
            Configuration conf = ctx.getConfiguration();
            block_size = conf.getInt("block_size", 32);
            isVector = conf.getBoolean("isVector", false);
        }

        @Override
        public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");

            if (isVector) {
                long row_id = Long.parseLong(line[0]);
                long block_id = row_id / block_size;
                long in_block_index = row_id % block_size;

                VALUE.setVector((int)in_block_index, Long.parseLong(line[1]));
                KEY.setVectorIndex(block_id);
                ctx.write(KEY, VALUE);
            } else {
                long row_id = Long.parseLong(line[0]);
                long col_id = Long.parseLong(line[1]);

                if (col_id == row_id) {
                    ctx.getCounter(PegasusCounter.NUMBER_SELF_LOOP).increment(1);
                    return;
                }
                long block_rowid = row_id / block_size;
                long block_colid = col_id / block_size;
                int in_block_row = (int) (row_id % block_size);
                int in_block_col = (int) (col_id % block_size);

                VALUE.setMatrix(in_block_row, in_block_col);
                KEY.setMatrixIndex(block_rowid, block_colid);

                ctx.write(KEY, VALUE);
                VALUE.setMatrix(in_block_col, in_block_row);
                KEY.setMatrixIndex(block_colid, block_rowid);
                ctx.write(KEY, VALUE);
            }
        }
    }

    public static class RedStage1 extends Reducer<BlockIndexWritable, LightBlockWritable, BlockIndexWritable, BlockWritable> {
        private BlockWritable VALUE = null;
        private int blockSize;
        private boolean isVector;

        @Override
        public void setup(Context ctx) {
            Configuration conf = ctx.getConfiguration();
            blockSize = conf.getInt("block_size", 32);
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

    protected Path pathEdges = null;
    protected Path pathOutput = null;
    protected int block_size = 1;
    protected int nreducer = 1;
    protected String output_prefix;
    protected boolean isVector = false;

    public static void main(final String[] args) throws Exception {
        final int result = ToolRunner.run(new Configuration(), new BlocksBuilder(), args);
        System.exit(result);
    }

    public int run(final String[] args) throws Exception {
        pathEdges = new Path(args[0]);
        pathOutput = new Path(args[1]);
        block_size = Integer.parseInt(args[2]);
        nreducer = Integer.parseInt(args[3]);

        isVector = args[4].equals("vector");

        System.out.println("\n-----===[PEGASUS: A Peta-Scale Graph Mining System]===-----\n");
        System.out.println("[PEGASUS] Converting the adjacency matrix to block format. Output_prefix = " + output_prefix + ", block width=" + block_size + "\n");

       if (!configStage1().waitForCompletion(true)) {
           System.err.println("Failed to execute BlocksBuilder");
           return -1;
       }

        System.out.println("\n[PEGASUS] Conversion finished.");
        System.out.println("[PEGASUS] Block adjacency matrix is saved in the HDFS " + args[1] + "\n");
        return 0;
    }

    protected Job configStage1() throws Exception {
        Configuration conf = getConf();
        conf.setInt("block_size", block_size);
        conf.setBoolean("isVector", isVector);
        conf.set("mapred.output.compression.type", "BLOCK"); // usefull ?

        Job job = new Job(conf, "MatvecPrep_Stage1");
        job.setJarByClass(BlocksBuilder.class);

        job.setMapperClass(MapStage1.class);
        job.setReducerClass(RedStage1.class);

        FileSystem fs = FileSystem.get(getConf());
        fs.delete(pathOutput, true);

        FileInputFormat.setInputPaths(job, pathEdges);
        SequenceFileOutputFormat.setOutputPath(job, pathOutput);
        SequenceFileOutputFormat.setCompressOutput(job, true);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setNumReduceTasks(nreducer);

        job.setMapOutputKeyClass(BlockIndexWritable.class);
        job.setMapOutputValueClass(LightBlockWritable.class);

        job.setOutputKeyClass(BlockIndexWritable.class);
        job.setOutputValueClass(BlockWritable.class);

        Runner.setCompression(job);

        return job;
    }
}

