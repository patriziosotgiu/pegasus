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
import gnu.trove.list.array.TLongArrayList;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.util.*;

public class ConCmptBlock extends Configured implements Tool {

    public static int MAX_ITERATIONS = 1024;

    static int iter_counter = 0;

    //
    // Stage1: group blocks by (matrix_column | vector_row) and compute the *
    //
    // Use secondary sorting so that values in the reducers are sorted by types: vector block then matrix blocks.
    //

    public static class Stage1JoinKey implements WritableComparable<Stage1JoinKey> {
        private boolean isVector;
        private long index;

        public Stage1JoinKey(boolean isVector, int index) {
            this.isVector = isVector;
            this.index = index;
        }

        public Stage1JoinKey() {
            this.isVector = false;
            this.index = -1;
        }

        @Override
        public int compareTo(Stage1JoinKey o) {
            int cmp = Long.compare(index, o.index);
            if (cmp != 0) {
                return cmp;
            }
            return - Boolean.compare(isVector, o.isVector);
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeBoolean(isVector);
            WritableUtils.writeVLong(dataOutput,index);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            isVector = dataInput.readBoolean();
            index = WritableUtils.readVLong(dataInput);
        }

        public void set(boolean isVector, long index) {
            this.isVector = isVector;
            this.index = index;
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

            Stage1JoinKey that = (Stage1JoinKey) o;

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

    public static class Stage1GroupComparator extends WritableComparator {

        protected Stage1GroupComparator() {
            super(Stage1JoinKey.class, true);
        }

        @Override
        public int compare(WritableComparable o1, WritableComparable o2) {
            Stage1JoinKey k1 = (Stage1JoinKey)o1;
            Stage1JoinKey k2 = (Stage1JoinKey)o2;
            return Long.compare(k1.index, k2.index);
        }
    }

    // TODO: use 2 distinct mappers and multiple input to avoid the if-else condition
    // output negative key to identify
    public static class MapStage1 extends MapReduceBase implements Mapper<BlockIndexWritable, BlockWritable, Stage1JoinKey, BlockWritable> {

        private static Stage1JoinKey KEY   = new Stage1JoinKey();
        private static BlockWritable VALUE = new BlockWritable();

        public void map(final BlockIndexWritable key, final BlockWritable value, final OutputCollector<Stage1JoinKey, BlockWritable> output, final Reporter reporter) throws IOException {
            VALUE.set(value);
            if (value.isTypeVector()) {
                KEY.set(true, key.getI());
            }
            else {
                KEY.set(false, key.getJ());
                VALUE.setBlockRow(key.getI());
            }
            output.collect(KEY, VALUE);
            //System.out.println("MapStage1.map: " + KEY + ", " + VALUE);
        }
    }

    public static class RedStage1 extends MapReduceBase implements Reducer<Stage1JoinKey, BlockWritable, LongWritable, BlockWritable> {
        protected int block_width;

        private BlockWritable initialVector = new BlockWritable();

        private LongWritable  KEY   = new LongWritable();
        private BlockWritable VALUE = new BlockWritable();

        public void configure(JobConf job) {
            block_width = Integer.parseInt(job.get("block_width"));
            System.out.println("RedStage1: block_width=" + block_width);
        }

        public void reduce(final Stage1JoinKey key, final Iterator<BlockWritable> values, OutputCollector<LongWritable, BlockWritable> output, final Reporter reporter) throws IOException {

            initialVector.set(values.next());
            //System.out.println("RedStage1.reduce input value: " + key + "," + initialVector);

            if (!initialVector.isTypeVector()) {
                // missing vector... should never happen, right ? throw exception ?
                reporter.incrCounter("ERROR", "no_vector", 1);
                System.err.println("error: no vector, key=" + key + ", first_value=" + initialVector);
                return;
            }

            VALUE.set(BlockWritable.TYPE.INITIAL, initialVector);
            KEY.set(key.index);
            output.collect(KEY, VALUE);
            //System.out.println("RedStage1.reduce: " + KEY + "," + VALUE);

            while (values.hasNext()) {
                BlockWritable e = values.next();
                //System.out.println("RedStage1.reduce input value: " + key + "," + e + ", initial vector: " + initialVector);
                KEY.set(e.getBlockRow());
                VALUE.setVector(BlockWritable.TYPE.INCOMPLETE, GIMV.minBlockVector(e, initialVector));
                output.collect(KEY, VALUE);
                //System.out.println("RedStage1.reduce: " + KEY + "," + VALUE);
            }
        }
    }

    //
    // Stage2: group blocks by row and compute the +
    //
    public static class RedStage2 extends MapReduceBase implements Reducer<LongWritable, BlockWritable, BlockIndexWritable, BlockWritable> {
        protected int block_width;

        private final BlockIndexWritable KEY = new BlockIndexWritable();
        private final BlockWritable VALUE = new BlockWritable();

        private TLongArrayList res = null;
        private BlockWritable initialVector = new BlockWritable();

        public void configure(JobConf job) {
            block_width = Integer.parseInt(job.get("block_width"));
            res = new TLongArrayList(block_width);
            System.out.println("RedStage2: block_width=" + block_width);
        }

        public void reduce(final LongWritable key, final Iterator<BlockWritable> values, final OutputCollector<BlockIndexWritable, BlockWritable> output, final Reporter reporter) throws IOException {
            boolean gotInitialVector = false;
            res.fill(0, block_width, -2);

            int n = 0;
            boolean isInitialVector = true;
            while (values.hasNext()) {
                BlockWritable block = values.next();
                // System.out.println("RedStage2.reduce input: " + key + "," + block);

                BlockWritable.TYPE t = block.getType();
                if (t == BlockWritable.TYPE.FINAL || t == BlockWritable.TYPE.INITIAL) {
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
                reporter.incrCounter("ERROR", "self_vector == null", 1);
                System.err.println("ERROR: self_vector == null, key=" + key + ", # values" + n);
                return;
            }

            boolean noChange = initialVector.getVectorElemValues().equals(res);
            BlockWritable.TYPE type = (noChange) ? BlockWritable.TYPE.FINAL : BlockWritable.TYPE.INCOMPLETE;
            VALUE.setVector(type, res);
            KEY.setVectorIndex(key.get());
            output.collect(KEY, VALUE);
            //System.out.println("RedStage2.reduce: " + KEY + "," + VALUE);
            reporter.incrCounter("change", noChange ? "final" : "incomplete", 1);
        }
    }

    //////////////////////////////////////////////////////////////////////
    // STAGE 4: Unfold the block component id format to plain format, after the bitstrings converged.
    //         This is a map-only stage.
    //  - Input: the converged component ids
    //  - Output: (node_id, "msu"component_id)
    //////////////////////////////////////////////////////////////////////
    public static class MapStage4 extends MapReduceBase implements Mapper<BlockIndexWritable, BlockWritable, LongWritable, Text> {
        int block_width;

        public void configure(JobConf job) {
            block_width = Integer.parseInt(job.get("block_width"));
            System.out.println("MapStage4: block_width = " + block_width);
        }

        // input sample :
        //1       msu0 1 1 1
        public void map(final BlockIndexWritable key, final BlockWritable value, final OutputCollector<LongWritable, Text> output, final Reporter reporter) throws IOException {
            long block_id = key.getI();
            for (int i = 0; i < value.getVectorElemValues().size(); i++) {
                long component_id = value.getVectorElemValues().get(i);
                if (component_id >= 0) {
                    output.collect(new LongWritable(block_width * block_id + i), new Text("msf" + component_id));
                }
            }
        }
    }

    //////////////////////////////////////////////////////////////////////
    // STAGE 5 : Summarize connected component information
    //    input : comcmpt_curbm (block format)
    //    output : comcmpt_summaryout
    //             min_node_id, number_of_nodes_in_the_component
    //////////////////////////////////////////////////////////////////////
    public static class MapStage5 extends MapReduceBase implements Mapper<BlockIndexWritable, BlockWritable, LongWritable, LongWritable> {
        private final LongWritable KEY = new LongWritable();
        private final LongWritable VALUE = new LongWritable(1);
        int block_width;

        public void configure(JobConf job) {
            block_width = Integer.parseInt(job.get("block_width"));
            System.out.println("MapStage5 : configure is called.  block_width=" + block_width);
        }

        public void map(final BlockIndexWritable key, final BlockWritable value, final OutputCollector<LongWritable, LongWritable> output, final Reporter reporter) throws IOException {
            for (int i = 0; i < value.getVectorElemValues().size(); i ++) {
                KEY.set(value.getVectorElemValues().get(i));
                output.collect(KEY, VALUE);
            }
        }
    }

    public static class RedStage5 extends MapReduceBase implements Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
        public void reduce(final LongWritable key, final Iterator<LongWritable> values, OutputCollector<LongWritable, LongWritable> output, final Reporter reporter) throws IOException {
            int count = 0;
            while (values.hasNext()) {
                count += values.next().get();
            }
            output.collect(key, new LongWritable(count));
        }
    }


    //////////////////////////////////////////////////////////////////////
    // command line interface
    //////////////////////////////////////////////////////////////////////
    protected Path edge_path = null;
    protected Path curbm_path = null;
    protected Path tempbm_path = null;
    protected Path nextbm_path = null;
    protected Path output_path = null;
    protected Path curbm_unfold_path = null;
    protected Path summaryout_path = null;
    protected String local_output_path;
    protected long number_nodes = 0;
    protected int nreducers = 1;
    protected int cur_radius = 0;
    protected int block_width = 64;
    protected int recursive_diagmult = 0;

    public static void main(final String[] args) throws Exception {
        final int result = ToolRunner.run(new Configuration(), new ConCmptBlock(), args);
        System.exit(result);
    }

    protected static int printUsage() {
        System.out.println("ConCmptBlock <edge_path> <curbm_path> <tempbm_path> <nextbm_path> <output_path> <# of nodes> <# of reducers> <fast or normal> <block_width>");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }

    public int run(final String[] args) throws Exception {
        if (args.length != 9) {
            return printUsage();
        }
        int i;

        edge_path = new Path(args[0]);
        curbm_path = new Path(args[1]);
        tempbm_path = new Path(args[2]);
        nextbm_path = new Path(args[3]);
        output_path = new Path(args[4]);
        curbm_unfold_path = new Path("concmpt_curbm");
        summaryout_path = new Path("concmpt_summaryout");
        number_nodes = Long.parseLong(args[5]);
        nreducers = Integer.parseInt(args[6]);

        if (args[7].compareTo("fast") == 0)
            recursive_diagmult = 1;
        else
            recursive_diagmult = 0;

        block_width = Integer.parseInt(args[8]);

        System.out.println("\n-----===[PEGASUS: A Peta-Scale Graph Mining System]===-----\n");
        System.out.println("[PEGASUS] Computing connected component using block method. Reducers = " + nreducers + ", block_width = " + block_width);

        local_output_path = args[4] + "_temp";

        final FileSystem fs = FileSystem.get(getConf());

        // Iteratively calculate neighborhood function.
        for (i = cur_radius; i < MAX_ITERATIONS; i++) {
            cur_radius++;
            iter_counter++;

            JobClient.runJob(configStage1());
            RunningJob j = JobClient.runJob(configStage2());

            long changed = j.getCounters().findCounter("change", "incomplete").getValue();
            long unchanged = j.getCounters().findCounter("change", "final").getValue();

            FileUtil.fullyDelete(fs, new Path(local_output_path));

            System.out.println("Hop " + i + " : changed = " + changed + ", unchanged = " + unchanged);

            // Stop when the minimum neighborhood doesn't change
            if (changed == 0) {
                System.out.println("All the component ids converged. Finishing...");
                fs.delete(curbm_path);
                fs.delete(tempbm_path);
                fs.delete(output_path);
                fs.rename(nextbm_path, curbm_path);

                System.out.println("Unfolding the block structure for easy lookup...");
                JobClient.runJob(configStage4());

                break;
            }

            // rotate directory
            fs.delete(curbm_path);
            fs.delete(tempbm_path);
            fs.delete(output_path);

            fs.rename(nextbm_path, curbm_path);
        }

        FileUtil.fullyDelete(FileSystem.getLocal(getConf()), new Path(local_output_path));

        // calculate summary information using an additional pass
        System.out.println("Summarizing connected components information...");
        JobClient.runJob(configStage5());

        // finishing.
        System.out.println("\n[PEGASUS] Connected component computed.");
        System.out.println("[PEGASUS] Total Iteration = " + iter_counter);
        System.out.println("[PEGASUS] Connected component information is saved in the HDFS concmpt_curbm as\n\"node_id	'msf'component_id\" format");
        System.out.println("[PEGASUS] Connected component distribution is saved in the HDFS concmpt_summaryout as\n\"component_id	number_of_nodes\" format.\n");

        return 0;
    }

    // Configure pass1
    protected JobConf configStage1() throws Exception {
        final JobConf conf = new JobConf(getConf(), ConCmptBlock.class);
        conf.set("block_width", "" + block_width);
        conf.set("recursive_diagmult", "" + recursive_diagmult);
        conf.setJobName("ConCmptBlock_pass1");

        conf.setMapperClass(MapStage1.class);
        conf.setReducerClass(RedStage1.class);

        FileInputFormat.setInputPaths(conf, edge_path, curbm_path);
        SequenceFileOutputFormat.setOutputPath(conf, tempbm_path);
        SequenceFileOutputFormat.setCompressOutput(conf, true);

        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        conf.set("mapred.output.compression.type", "BLOCK");

        conf.setNumReduceTasks(nreducers);

        conf.setMapOutputKeyClass(Stage1JoinKey.class);
        conf.setMapOutputValueClass(BlockWritable.class);
        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(BlockWritable.class);
        conf.setOutputValueGroupingComparator(Stage1GroupComparator.class);

        setCompression(conf);

        return conf;
    }

    // Configure pass2
    protected JobConf configStage2() throws Exception {
        final JobConf conf = new JobConf(getConf(), ConCmptBlock.class);
        conf.set("block_width", "" + block_width);
        conf.setJobName("ConCmptBlock_pass2");

        conf.setMapperClass(IdentityMapper.class);
        conf.setReducerClass(RedStage2.class);

        SequenceFileInputFormat.setInputPaths(conf, tempbm_path);
        FileOutputFormat.setOutputPath(conf, nextbm_path);
        FileOutputFormat.setCompressOutput(conf, true);

        conf.setNumReduceTasks(nreducers);

        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);

        conf.setMapOutputKeyClass(LongWritable.class);
        conf.setMapOutputValueClass(BlockWritable.class);
        conf.setOutputKeyClass(BlockIndexWritable.class);
        conf.setOutputValueClass(BlockWritable.class);

        setCompression(conf);
        return conf;
    }

    // Configure pass4
    protected JobConf configStage4() throws Exception {
        final JobConf conf = new JobConf(getConf(), ConCmptBlock.class);
        conf.set("block_width", "" + block_width);
        conf.setJobName("ConCmptBlock_pass4");

        conf.setMapperClass(MapStage4.class);

        FileInputFormat.setInputPaths(conf, curbm_path);
        FileOutputFormat.setOutputPath(conf, curbm_unfold_path);
        FileOutputFormat.setCompressOutput(conf, true);

        conf.setInputFormat(SequenceFileInputFormat.class);

        conf.setNumReduceTasks(0);        //This is essential for map-only tasks.

        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(Text.class);

        setCompression(conf);
        return conf;
    }

    // Configure pass5
    protected JobConf configStage5() throws Exception {
        final JobConf conf = new JobConf(getConf(), ConCmptBlock.class);
        conf.set("block_width", "" + block_width);
        conf.setJobName("ConCmptBlock_pass5");

        conf.setMapperClass(MapStage5.class);
        conf.setReducerClass(RedStage5.class);
        conf.setCombinerClass(RedStage5.class);

        FileInputFormat.setInputPaths(conf, curbm_path);
        FileOutputFormat.setOutputPath(conf, summaryout_path);
        FileOutputFormat.setCompressOutput(conf, true);

        conf.setInputFormat(SequenceFileInputFormat.class);

        conf.setNumReduceTasks(nreducers);

        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(LongWritable.class);

        setCompression(conf);
        return conf;
    }

    public static void setCompression(JobConf conf) {
     //   FileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
         FileOutputFormat.setOutputCompressorClass(conf, SnappyCodec.class);
    }
}
