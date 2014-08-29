/***********************************************************************
 PEGASUS: Peta-Scale Graph Mining System
 Authors: U Kang, Duen Horng Chau, and Christos Faloutsos

 This software is licensed under Apache License, Version 2.0 (the  "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 -------------------------------------------------------------------------
 File: ConCmptBlock.java
 - HCC: Find Connected Components of graph using block multiplication. This is a block-based version of HCC.
 Version: 2.0
 ***********************************************************************/

package pegasus;

import java.io.*;
import java.util.*;

import gnu.trove.list.array.TLongArrayList;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.util.*;

public class ConCmptBlock extends Configured implements Tool {
    public static int MAX_ITERATIONS = 1024;
    public static long changed_nodes[] = new long[MAX_ITERATIONS];

    static int iter_counter = 0;

    private static LongWritable KEY = new LongWritable();
    private static BlockWritable VALUE = new BlockWritable();

    public static class MapStage1 extends MapReduceBase implements Mapper<BlockIndexWritable, BlockWritable, LongWritable, BlockWritable> {
        public void map(final BlockIndexWritable key, final BlockWritable value, final OutputCollector<LongWritable, BlockWritable> output, final Reporter reporter) throws IOException {
            VALUE.set(value);
            if (value.isTypeVector()) {
                KEY.set(key.getI());
            }
            else {
                KEY.set(key.getJ());
                VALUE.setBlockRow(key.getI());
            }
            output.collect(KEY, VALUE);
        }
    }

    public static class RedStage1 extends MapReduceBase implements Reducer<LongWritable, BlockWritable, LongWritable, BlockWritable> {
        protected int block_width;
        protected int recursive_diagmult;

        private BlockWritable            initialVector = new BlockWritable();
        private ArrayList<BlockWritable> blocks        = new ArrayList<BlockWritable>();
        private TLongArrayList           blockRows     = new TLongArrayList();

        private LongWritable  KEY   = new LongWritable();
        private BlockWritable VALUE = new BlockWritable();

        public void configure(JobConf job) {
            block_width = Integer.parseInt(job.get("block_width"));
            recursive_diagmult = Integer.parseInt(job.get("recursive_diagmult"));
            System.out.println("RedStage1: block_width=" + block_width + ", recursive_diagmult=" + recursive_diagmult);
        }

        public void reduce(final LongWritable key, final Iterator<BlockWritable> values, OutputCollector<LongWritable, BlockWritable> output, final Reporter reporter) throws IOException {
            boolean gotVectorArr = false;
            blocks.clear();
            blockRows.clear();

            // todo: use secondary sorting instead
            while (values.hasNext()) {
                BlockWritable e = values.next();
                if (e.isTypeVector()) {
                    initialVector.set(e);
                    gotVectorArr = true;
                }
                else {
                    blocks.add(new BlockWritable(e));
                    blockRows.add(e.getBlockRow());
                }
            }

            if (!gotVectorArr) // missing vector.
                return;

            // output 'self' block to check convergence
            VALUE.set(BlockWritable.TYPE.INITIAL, initialVector);
            output.collect(key, VALUE);

            // For every matrix block, join it with vector and output partial results
            Iterator<BlockWritable> blockArrIter = blocks.iterator();
            TLongArrayList res;
            for (int i = 0; i < blockRows.size(); i++) {
                res = GIMV.minBlockVector(blockArrIter.next(), initialVector);
                if (res != null && res.size() > 0) {  // fixme: don't output if useless vector
                    KEY.set(blockRows.get(i));
                    VALUE.setVector(BlockWritable.TYPE.INCOMPLETE, res);
                    output.collect(KEY, VALUE);
                }
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // STAGE 2 merge partial comonent ids.
    //  - Input: partial component ids
    //  - Output: combined component ids
    ////////////////////////////////////////////////////////////////////////////////////////////////

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

        // Key: block row id
        // Value: list of vector blocks
        public void reduce(final LongWritable key, final Iterator<BlockWritable> values, final OutputCollector<BlockIndexWritable, BlockWritable> output, final Reporter reporter) throws IOException {
            boolean gotInitialVector = false;
            res.fill(0, block_width, -1);

            int n = 0;
            while (values.hasNext()) {
                BlockWritable block = values.next();

                BlockWritable.TYPE t = block.getType();
                if (t == BlockWritable.TYPE.FINAL || t == BlockWritable.TYPE.INITIAL) {
                    initialVector.set(block);
                    gotInitialVector = true;
                }

                for (int i = 0; i < block.getVectorElemValues().size(); i++) {
                    long v = block.getVectorElemValues().getQuick(i);
                    if (res.getQuick(i) == -1) {
                        res.setQuick(i, v);
                    }
                    else if (v < res.getQuick(i))  {
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
        }
    }

    //////////////////////////////////////////////////////////////////////
    // STAGE 3: Calculate number of nodes whose component id changed/unchanged.
    //  - Input: current component ids
    //  - Output: number_of_changed_nodes
    //////////////////////////////////////////////////////////////////////
    public static class MapStage3 extends MapReduceBase implements Mapper<BlockIndexWritable, BlockWritable, Text, Text> {
        // output : f n		( n : # of node whose component didn't change)
        //          i m		( m : # of node whose component changed)
        public void map(final BlockIndexWritable key, final BlockWritable value, final OutputCollector<Text, Text> output, final Reporter reporter) throws IOException {
            char change_prefix = value.getType().toString().toLowerCase().charAt(2);
            output.collect(new Text(Character.toString(change_prefix)), new Text("1"));
        }
    }

    public static class RedStage3 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        public void reduce(final Text key, final Iterator<Text> values, final OutputCollector<Text, Text> output, final Reporter reporter) throws IOException {
            long sum = 0;
            while (values.hasNext()) {
                sum += Long.parseLong(values.next().toString());
            }
            output.collect(key, new Text(Long.toString(sum)));
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
                output.collect(new LongWritable(block_width * block_id + i), new Text("msf" + component_id));
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

        // Iteratively calculate neighborhood function.
        for (i = cur_radius; i < MAX_ITERATIONS; i++) {
            cur_radius++;
            iter_counter++;

            JobClient.runJob(configStage1());
            JobClient.runJob(configStage2());
            JobClient.runJob(configStage3());

            FileUtil.fullyDelete(FileSystem.getLocal(getConf()), new Path(local_output_path));

            final FileSystem fs = FileSystem.get(getConf());

            // copy neighborhood information from HDFS to local disk, and read it!
            String new_path = local_output_path + "/" + i;
            fs.copyToLocalFile(output_path, new Path(new_path));
            ResultInfo ri = ConCmpt.readIterationOutput(new_path);

            changed_nodes[iter_counter] = ri.changed;
            changed_nodes[iter_counter] = ri.unchanged;

            System.out.println("Hop " + i + " : changed = " + ri.changed + ", unchanged = " + ri.unchanged);

            // Stop when the minimum neighborhood doesn't change
            if (ri.changed == 0) {
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
        //FileOutputFormat.setOutputCompressorClass(conf, SnappyCodec.class);

        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        conf.set("mapred.output.compression.type", "BLOCK");

        conf.setNumReduceTasks(nreducers);

        conf.setMapOutputKeyClass(LongWritable.class);
        conf.setMapOutputValueClass(BlockWritable.class);
        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(BlockWritable.class);

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
        //FileOutputFormat.setOutputCompressorClass(conf, SnappyCodec.class);

        conf.setNumReduceTasks(nreducers);

        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);

        conf.setMapOutputKeyClass(LongWritable.class);
        conf.setMapOutputValueClass(BlockWritable.class);
        conf.setOutputKeyClass(BlockIndexWritable.class);
        conf.setOutputValueClass(BlockWritable.class);

        return conf;
    }

    // Configure pass3
    protected JobConf configStage3() throws Exception {
        final JobConf conf = new JobConf(getConf(), ConCmptBlock.class);
        conf.setJobName("ConCmptBlock_pass3");

        conf.setMapperClass(MapStage3.class);
        conf.setReducerClass(RedStage3.class);
        conf.setCombinerClass(RedStage3.class);

        FileInputFormat.setInputPaths(conf, nextbm_path);
        FileOutputFormat.setOutputPath(conf, output_path);

        conf.setNumReduceTasks(1);// This is necessary to summarize and save data.

        conf.setInputFormat(SequenceFileInputFormat.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

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
       // FileOutputFormat.setOutputCompressorClass(conf, SnappyCodec.class);

        conf.setInputFormat(SequenceFileInputFormat.class);

        conf.setNumReduceTasks(0);        //This is essential for map-only tasks.

        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(Text.class);

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
     //   FileOutputFormat.setOutputCompressorClass(conf, SnappyCodec.class);

        conf.setInputFormat(SequenceFileInputFormat.class);

        conf.setNumReduceTasks(nreducers);

        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(LongWritable.class);

        return conf;
    }
}