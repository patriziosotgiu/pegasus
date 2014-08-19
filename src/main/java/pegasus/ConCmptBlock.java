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

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.util.*;

public class ConCmptBlock extends Configured implements Tool {
    public static int MAX_ITERATIONS = 1024;
    public static long changed_nodes[] = new long[MAX_ITERATIONS];

    static int iter_counter = 0;

    private static LongWritable KEY = new LongWritable();
    private static ElemArrayWritable VALUE = new ElemArrayWritable();


    //////////////////////////////////////////////////////////////////////
    // STAGE 1: generate partial block-component ids.
    //          Hash-join edge and vector by Vector.BLOCKROWID == Edge.BLOCKCOLID where
    //          vector: key=BLOCKID, value= msu (IN-BLOCK-INDEX VALUE)s
    //                                      moc
    //          edge: key=BLOCK-ROW		BLOCK-COL, value=(IN-BLOCK-ROW IN-BLOCK-COL VALUE)s
    //  - Input: edge_file, component_ids_from_the_last_iteration
    //  - Output: partial component ids
    //////////////////////////////////////////////////////////////////////
    public static class MapStage1 extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, ElemArrayWritable> {
        public void map(final LongWritable key, final Text value, final OutputCollector<LongWritable, ElemArrayWritable> output, final Reporter reporter) throws IOException {
            String line_text = value.toString();
            if (line_text.startsWith("#"))                // ignore comments in edge file
                return;

            final String[] line = line_text.split("\t");

            if (line.length < 2)
                return;

            VALUE.reset();

            long v1 = Long.parseLong(line[0]);
            if (line.length == 2) {    // vector. component information.
                KEY.set(v1);
                String tokens[] = line[1].substring(3).split(" ");
                for (int i = 0; i < tokens.length; i += 2) {
                    VALUE.addVector(Short.parseShort(tokens[i]), Long.parseLong(tokens[i + 1]));
                }
            } else {                    // edge
                long v2 = Long.parseLong(line[1]);
                KEY.set(v2);
                String tokens[] = line[2].split(" ");
                VALUE.setBlockCol(v1);
                for (int i = 0; i < tokens.length; i += 2) {
                    VALUE.addBlock(Short.parseShort(tokens[i + 1]), Short.parseShort(tokens[i]), 1);
                }
            }
            output.collect(KEY, VALUE);
        }
    }

    public static class RedStage1 extends MapReduceBase implements Reducer<LongWritable, ElemArrayWritable, LongWritable, VectorElemWritable> {
        protected int block_width;
        protected int recursive_diagmult;

        private ArrayList<VectorElem>          vectorArr = null;                                   // save vector
        private ArrayList<ArrayList<BlockElem>> blockArr = new ArrayList<ArrayList<BlockElem>>();  // save blocks
        private ArrayList<Long>              blockRowArr = new ArrayList<Long>();    // save block rows(integer)

        private LongWritable KEY = new LongWritable();
        private VectorElemWritable VALUE = new VectorElemWritable();

        public void configure(JobConf job) {
            block_width = Integer.parseInt(job.get("block_width"));
            recursive_diagmult = Integer.parseInt(job.get("recursive_diagmult"));
            System.out.println("RedStage1: block_width=" + block_width + ", recursive_diagmult=" + recursive_diagmult);
        }

        public void reduce(final LongWritable key, final Iterator<ElemArrayWritable> values, OutputCollector<LongWritable, VectorElemWritable> output, final Reporter reporter) throws IOException {
            vectorArr = null;
            blockArr.clear();
            blockRowArr.clear();

            while (values.hasNext()) {
                ElemArrayWritable e = values.next();
                if (!e.getVectors().isEmpty()) {
                    vectorArr = new ArrayList<VectorElem>(e.getVectors());
                }
                else {
                    blockArr.add(e.getBlocks());
                    blockRowArr.add(e.getBlockCol());
                }
            }

            if (vectorArr == null) // missing vector.
                return;

            // output 'self' block to check convergence
            VALUE.set(VectorElemWritable.TYPE.MSI, vectorArr);
            output.collect(key, VALUE);

            // For every matrix block, join it with vector and output partial results
            Iterator<ArrayList<BlockElem>> blockArrIter = blockArr.iterator();
            Iterator<Long> blockRowIter = blockRowArr.iterator();
            while (blockArrIter.hasNext()) {

                ArrayList<BlockElem> cur_block = blockArrIter.next();
                long cur_block_row = blockRowIter.next();

                ArrayList<VectorElem> cur_mult_result = null;

                if (key.get() == cur_block_row && recursive_diagmult == 1) {    // do recursive multiplication
                    ArrayList<VectorElem> tempVectorArr = vectorArr;
                    for (int i = 0; i < block_width; i++) {
                        cur_mult_result = GIMV.minBlockVector(cur_block, tempVectorArr, block_width, 1);
                        if (cur_mult_result == null || GIMV.compareVectors(tempVectorArr, cur_mult_result) == 0)
                            break;

                        tempVectorArr = cur_mult_result;
                    }
                } else {
                    cur_mult_result = GIMV.minBlockVector(cur_block, vectorArr, block_width, 0);
                }
                if (cur_mult_result != null && cur_mult_result.size() > 0) {
                    KEY.set(cur_block_row);
                    VALUE.set(VectorElemWritable.TYPE.MOI, cur_mult_result);
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

    public static class RedStage2 extends MapReduceBase implements Reducer<LongWritable, VectorElemWritable, LongWritable, VectorElemWritable> {
        protected int block_width;

        private final VectorElemWritable VALUE = new VectorElemWritable();

        public void configure(JobConf job) {
            block_width = Integer.parseInt(job.get("block_width"));
            System.out.println("RedStage2: block_width=" + block_width);
        }

        public void reduce(final LongWritable key, final Iterator<VectorElemWritable> values, final OutputCollector<LongWritable, VectorElemWritable> output, final Reporter reporter) throws IOException {
            ArrayList<VectorElem> self_vector = null;
            long[] out_vals = new long[block_width];
            for (int i = 0; i < block_width; i++)
                out_vals[i] = -1;

            int n = 0;
            while (values.hasNext()) {
                VectorElemWritable cur_val = values.next();

                if (cur_val.getType() == VectorElemWritable.TYPE.MSF
                        || cur_val.getType() == VectorElemWritable.TYPE.MSI)
                {
                    self_vector = new ArrayList<VectorElem>(cur_val.getVector());
                }

                for (VectorElem v_elem : cur_val.getVector()) {
                    if (out_vals[v_elem.row] == -1)
                        out_vals[v_elem.row] = v_elem.val;
                    else if (out_vals[v_elem.row] > v_elem.val)
                        out_vals[v_elem.row] = v_elem.val;
                }
                n++;
            }

            if (self_vector == null) {
                reporter.incrCounter("ERROR", "self_vector == null", 1);
                System.err.println("ERROR: self_vector == null, key=" + key + ", # values" + n);
                return;
            }
            ArrayList<VectorElem> new_vector = GIMV.makeLongVectors(out_vals, block_width);
            VectorElemWritable.TYPE type;
            if (1 == GIMV.compareVectors(self_vector, new_vector)) {
                type = VectorElemWritable.TYPE.MSI;
            }
            else {
                type = VectorElemWritable.TYPE.MSF;
            }
            VALUE.set(type, new_vector);
            output.collect(key, VALUE);
        }
    }

    //////////////////////////////////////////////////////////////////////
    // STAGE 3: Calculate number of nodes whose component id changed/unchanged.
    //  - Input: current component ids
    //  - Output: number_of_changed_nodes
    //////////////////////////////////////////////////////////////////////
    public static class MapStage3 extends MapReduceBase implements Mapper<LongWritable, VectorElemWritable, Text, Text> {
        // output : f n		( n : # of node whose component didn't change)
        //          i m		( m : # of node whose component changed)
        public void map(final LongWritable key, final VectorElemWritable value, final OutputCollector<Text, Text> output, final Reporter reporter) throws IOException {
            char change_prefix = value.getType().toString().toLowerCase().charAt(2);
            output.collect(new Text(Character.toString(change_prefix)), new Text("1"));
        }
    }

    public static class RedStage3 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        public void reduce(final Text key, final Iterator<Text> values, final OutputCollector<Text, Text> output, final Reporter reporter) throws IOException {
            long sum = 0;

            while (values.hasNext()) {
                final String line = values.next().toString();
                long cur_value = Long.parseLong(line);

                sum += cur_value;
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
    public static class MapStage4 extends MapReduceBase implements Mapper<LongWritable, VectorElemWritable, LongWritable, Text> {
        int block_width;

        public void configure(JobConf job) {
            block_width = Integer.parseInt(job.get("block_width"));

            System.out.println("MapStage4: block_width = " + block_width);
        }

        // input sample :
        //1       msu0 1 1 1
        public void map(final LongWritable key, final VectorElemWritable value, final OutputCollector<LongWritable, Text> output, final Reporter reporter) throws IOException {
            long block_id = key.get();
            for (VectorElem e: value.getVector()) {
                long elem_row = e.row;
                long component_id = e.val;
                output.collect(new LongWritable(block_width * block_id + elem_row), new Text("msf" + component_id));
            }
        }
    }

    //////////////////////////////////////////////////////////////////////
    // STAGE 5 : Summarize connected component information
    //    input : comcmpt_curbm (block format)
    //    output : comcmpt_summaryout
    //             min_node_id, number_of_nodes_in_the_component
    //////////////////////////////////////////////////////////////////////
    public static class MapStage5 extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, LongWritable> {
        private final LongWritable out_key_int = new LongWritable();
        private final LongWritable out_count_int = new LongWritable(1);
        int block_width;

        public void configure(JobConf job) {
            block_width = Integer.parseInt(job.get("block_width"));
            System.out.println("MapStage5 : configure is called.  block_width=" + block_width);
        }

        public void map(final LongWritable key, final Text value, final OutputCollector<LongWritable, LongWritable> output, final Reporter reporter) throws IOException {
            String line_text = value.toString();
            final String[] line = line_text.split("\t");
            final String[] elems = line[1].substring(3).split(" ");

            for (int i = 0; i < elems.length; i += 2) {
                long cur_minnode = Long.parseLong(elems[i + 1]);

                out_key_int.set(cur_minnode);
                output.collect(out_key_int, out_count_int);
            }
        }
    }

    public static class RedStage5 extends MapReduceBase implements Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
        public void reduce(final LongWritable key, final Iterator<LongWritable> values, OutputCollector<LongWritable, LongWritable> output, final Reporter reporter) throws IOException {
            int count = 0;

            while (values.hasNext()) {
                long cur_count = values.next().get();
                count += cur_count;
            }

            LongWritable count_int = new LongWritable(count);
            output.collect(key, count_int);
        }
    }


    //////////////////////////////////////////////////////////////////////
    // command line interface
    //////////////////////////////////////////////////////////////////////
    protected Path edge_path = null;
    protected Path vector_path = null;
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

    // Main entry point.
    public static void main(final String[] args) throws Exception {
        final int result = ToolRunner.run(new Configuration(), new ConCmptBlock(), args);

        System.exit(result);
    }


    // Print the command-line usage text.
    protected static int printUsage() {
        System.out.println("ConCmptBlock <edge_path> <curbm_path> <tempbm_path> <nextbm_path> <output_path> <# of nodes> <# of reducers> <fast or normal> <block_width>");

        ToolRunner.printGenericCommandUsage(System.out);

        return -1;
    }

    // submit the map/reduce job.
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

        conf.setOutputFormat(SequenceFileOutputFormat.class);
        conf.set("mapred.output.compression.type", "BLOCK");

        conf.setNumReduceTasks(nreducers);

        conf.setMapOutputKeyClass(LongWritable.class);
        conf.setMapOutputValueClass(ElemArrayWritable.class);
        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(VectorElemWritable.class);

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
        conf.setMapOutputValueClass(VectorElemWritable.class);
        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(VectorElemWritable.class);

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
        conf.setOutputFormat(SequenceFileOutputFormat.class);

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
        conf.setOutputFormat(SequenceFileOutputFormat.class);

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
        conf.setOutputFormat(SequenceFileOutputFormat.class);

        conf.setNumReduceTasks(nreducers);

        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(LongWritable.class);

        return conf;
    }
}

