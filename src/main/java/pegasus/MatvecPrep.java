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
import java.util.*;

import com.google.common.base.Objects;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class MatvecPrep extends Configured implements Tool {

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


    public static class MapStage1 extends MapReduceBase implements Mapper<LongWritable, Text, BlockIndexWritable, LightBlockWritable> {
        private int block_size;
        private int makesym;
        private boolean isVector;

        private final BlockIndexWritable KEY = new BlockIndexWritable();
        private final LightBlockWritable VALUE = new LightBlockWritable();

        public void configure(JobConf job) {
            block_size = Integer.parseInt(job.get("block_size"));
            makesym = Integer.parseInt(job.get("makesym"));
            isVector = job.getBoolean("isVector", false);
            System.out.println("MapStage1: block_size = " + block_size + ", makesym = " + makesym);
        }

        public void map(final LongWritable key, final Text value, final OutputCollector<BlockIndexWritable, LightBlockWritable> output, final Reporter reporter) throws IOException {
            String[] line = value.toString().split("\t");

            if (isVector) {
                long row_id = Long.parseLong(line[0]);
                long block_id = row_id / block_size;
                long in_block_index = row_id % block_size;

                VALUE.setVector((int)in_block_index, Long.parseLong(line[1]));
                KEY.setVectorIndex(block_id);
                output.collect(KEY, VALUE);
            } else {
                long row_id = Long.parseLong(line[0]);
                long col_id = Long.parseLong(line[1]);

                if (col_id == row_id) {
                    reporter.incrCounter("PEGASUS", "Number of self edges removed", 1);
                    return;
                }
                long block_rowid = row_id / block_size;
                long block_colid = col_id / block_size;
                int in_block_row = (int) (row_id % block_size);  // get rided of transpose ? safe ?
                int in_block_col = (int) (col_id % block_size); // get rided of transpose ? safe ?

                VALUE.setMatrix(in_block_row, in_block_col);
                KEY.setMatrixIndex(block_rowid, block_colid);

                output.collect(KEY, VALUE);
                if (makesym == 1)
                {
                    VALUE.setMatrix(in_block_col, in_block_row);
                    KEY.setMatrixIndex(block_colid, block_rowid);
                    output.collect(KEY, VALUE);
                }
            }
        }
    }

    public static class RedStage1 extends MapReduceBase implements Reducer<BlockIndexWritable, LightBlockWritable, BlockIndexWritable, BlockWritable> {
        private BlockWritable VALUE = null;
        private int blockSize;
        private boolean isVector;

        public void configure(JobConf job) {
            blockSize = Integer.parseInt(job.get("block_size"));
            isVector = job.getBoolean("isVector", false);
            VALUE = new BlockWritable(blockSize, isVector ? BlockWritable.TYPE.VECTOR_INITIAL : BlockWritable.TYPE.MATRIX);
        }

        public void reduce(final BlockIndexWritable key, final Iterator<LightBlockWritable> values, final OutputCollector<BlockIndexWritable, BlockWritable> output, final Reporter reporter) throws IOException {
            if (isVector) {
                VALUE.resetVector();
                boolean initVector = false;
                while (values.hasNext()) {
                    LightBlockWritable block = values.next();
                    if (!initVector) {
                        VALUE.setVectorInitialValue(blockSize);
                        initVector = true;
                    }
                    VALUE.setVectorElem(block.index1, block.value);
                }
                output.collect(key, VALUE);
            }
            else {
                VALUE.resetMatrix();
                while (values.hasNext()) {
                    LightBlockWritable block = values.next();
                    VALUE.addMatrixElem(block.index1, block.index2);
                }
                output.collect(key, VALUE);
            }
        }
    }

    protected Path pathEdges = null;
    protected Path pathOutput = null;
    protected int block_size = 1;
    protected int nreducer = 1;
    protected String output_prefix;
    protected int makesym = 0;
    protected boolean isVector = false;

    public static void main(final String[] args) throws Exception {
        final int result = ToolRunner.run(new Configuration(), new MatvecPrep(), args);
        System.exit(result);
    }

    public int run(final String[] args) throws Exception {
        pathEdges = new Path(args[0]);
        pathOutput = new Path(args[1]);
        block_size = Integer.parseInt(args[2]);
        nreducer = Integer.parseInt(args[3]);

        makesym = 1;
        isVector = args[4].equals("vector");

        System.out.println("\n-----===[PEGASUS: A Peta-Scale Graph Mining System]===-----\n");
        System.out.println("[PEGASUS] Converting the adjacency matrix to block format. Output_prefix = " + output_prefix + ", makesym = " + makesym + ", block width=" + block_size + "\n");

        JobClient.runJob(configStage1());

        System.out.println("\n[PEGASUS] Conversion finished.");
        System.out.println("[PEGASUS] Block adjacency matrix is saved in the HDFS " + args[1] + "\n");
        return 0;
    }

    protected JobConf configStage1() throws Exception {
        final JobConf conf = new JobConf(getConf(), MatvecPrep.class);
        conf.set("block_size", "" + block_size);
        conf.set("makesym", "" + makesym);
        conf.setBoolean("isVector", isVector);

        conf.setJobName("MatvecPrep_Stage1");

        conf.setMapperClass(MapStage1.class);
        conf.setReducerClass(RedStage1.class);

        FileSystem fs = FileSystem.get(getConf());
        fs.delete(pathOutput, true);

        FileInputFormat.setInputPaths(conf, pathEdges);
        SequenceFileOutputFormat.setOutputPath(conf, pathOutput);
        SequenceFileOutputFormat.setCompressOutput(conf, true);

        conf.setOutputFormat(SequenceFileOutputFormat.class);
        conf.set("mapred.output.compression.type", "BLOCK");

        conf.setNumReduceTasks(nreducer);

        conf.setMapOutputKeyClass(BlockIndexWritable.class);
        conf.setMapOutputValueClass(LightBlockWritable.class);

        conf.setOutputKeyClass(BlockIndexWritable.class);
        conf.setOutputValueClass(BlockWritable.class);

        Runner.setCompression(conf);

        return conf;
    }
}

