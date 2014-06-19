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
import org.apache.hadoop.util.*;

public class ConCmptBlock extends Configured implements Tool {
    public static int MAX_ITERATIONS = 1024;
    public static long changed_nodes[] = new long[MAX_ITERATIONS];

    static int iter_counter = 0;


    //////////////////////////////////////////////////////////////////////
    // STAGE 1: generate partial block-component ids.
    //          Hash-join edge and vector by Vector.BLOCKROWID == Edge.BLOCKCOLID where
    //          vector: key=BLOCKID, value= msu (IN-BLOCK-INDEX VALUE)s
    //                                      moc
    //          edge: key=BLOCK-ROW		BLOCK-COL, value=(IN-BLOCK-ROW IN-BLOCK-COL VALUE)s
    //  - Input: edge_file, component_ids_from_the_last_iteration
    //  - Output: partial component ids
    //////////////////////////////////////////////////////////////////////
    public static class MapStage1 extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
        public void map(final LongWritable key, final Text value, final OutputCollector<LongWritable, Text> output, final Reporter reporter) throws IOException {
            String line_text = value.toString();
            if (line_text.startsWith("#"))                // ignore comments in edge file
                return;

            final String[] line = line_text.split("\t");

            if (line.length < 2)
                return;

            if (line.length == 2) {    // vector. component information.
                output.collect(new LongWritable(Long.parseLong(line[0])), new Text(line[1]));
            } else {                    // edge
                output.collect(new LongWritable(Long.parseLong(line[1])), new Text(line[0] + "\t" + line[2]));
            }
        }
    }

    public static class RedStage1 extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {
        protected int block_width;
        protected int recursive_diagmult;

        public void configure(JobConf job) {
            block_width = Integer.parseInt(job.get("block_width"));
            recursive_diagmult = Integer.parseInt(job.get("recursive_diagmult"));
            System.out.println("RedStage1: block_width=" + block_width + ", recursive_diagmult=" + recursive_diagmult);
        }

        public void reduce(final LongWritable key, final Iterator<Text> values, OutputCollector<LongWritable, Text> output, final Reporter reporter) throws IOException {
            ArrayList<VectorElem<Long>> vectorArr = null;        // save vector
            ArrayList<ArrayList<BlockElem<Long>>> blockArr = new ArrayList<ArrayList<BlockElem<Long>>>();    // save blocks
            ArrayList<Long> blockRowArr = new ArrayList<Long>();    // save block rows(integer)

            while (values.hasNext()) {
                // vector: key=BLOCKID, value= (IN-BLOCK-INDEX VALUE)s
                // edge: key=BLOCK-COLID	BLOCK-ROWID, value=(IN-BLOCK-COL IN-BLOCK-ROW VALUE)s
                String line_text = values.next().toString();
                final String[] line = line_text.split("\t");

                if (line.length == 1) {    // vector : VALUE
                    vectorArr = GIMV.parseVectorVal(line_text.substring(3), Long.class);
                } else {                    // edge : BLOCK-ROWID		VALUE
                    blockArr.add(GIMV.parseBlockVal(line[1], Long.class));
                    long block_row = Long.parseLong(line[0]);
                    blockRowArr.add(block_row);
                }
            }

            int blockCount = blockArr.size();
            if (vectorArr == null)// || blockCount == 0 ) // missing vector or block.
                return;

            // output 'self' block to check convergence
            output.collect(key, GIMV.formatVectorElemOutput("msi", vectorArr));

            // For every matrix block, join it with vector and output partial results
            Iterator<ArrayList<BlockElem<Long>>> blockArrIter = blockArr.iterator();
            Iterator<Long> blockRowIter = blockRowArr.iterator();
            while (blockArrIter.hasNext()) {
                ArrayList<BlockElem<Long>> cur_block = blockArrIter.next();
                long cur_block_row = blockRowIter.next();

                ArrayList<VectorElem<Long>> cur_mult_result = null;

                if (key.get() == cur_block_row && recursive_diagmult == 1) {    // do recursive multiplication
                    ArrayList<VectorElem<Long>> tempVectorArr = vectorArr;
                    for (int i = 0; i < block_width; i++) {
                        cur_mult_result = GIMV.minBlockVector(cur_block, tempVectorArr, block_width, 1);
                        if (cur_mult_result == null || GIMV.compareVectors(tempVectorArr, cur_mult_result) == 0)
                            break;

                        tempVectorArr = cur_mult_result;
                    }
                } else {
                    cur_mult_result = GIMV.minBlockVector(cur_block, vectorArr, block_width, 0);
                }

                Text output_vector = GIMV.formatVectorElemOutput("moi", cur_mult_result);
                if (output_vector.toString().length() > 0)
                    output.collect(new LongWritable(cur_block_row), output_vector);
            }
        }

    }


    ////////////////////////////////////////////////////////////////////////////////////////////////
    // STAGE 2: merge partial comonent ids.
    //  - Input: partial component ids
    //  - Output: combined component ids
    ////////////////////////////////////////////////////////////////////////////////////////////////
    public static class MapStage2 extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
        // Identity mapper
        public void map(final LongWritable key, final Text value, final OutputCollector<LongWritable, Text> output, final Reporter reporter) throws IOException {
            final String[] line = value.toString().split("\t");

            output.collect(new LongWritable(Long.parseLong(line[0])), new Text(line[1]));
        }
    }

    public static class RedStage2 extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {
        protected int block_width;

        public void configure(JobConf job) {
            block_width = Integer.parseInt(job.get("block_width"));
            System.out.println("RedStage2: block_width=" + block_width);
        }

        public void reduce(final LongWritable key, final Iterator<Text> values, final OutputCollector<LongWritable, Text> output, final Reporter reporter) throws IOException {
            ArrayList<VectorElem<Long>> self_vector = null;
            long[] out_vals = new long[block_width];
            for (int i = 0; i < block_width; i++)
                out_vals[i] = -1;

            int n = 0;
            while (values.hasNext()) {
                String cur_str = values.next().toString();

                if (cur_str.charAt(1) == 's') {
                    self_vector = GIMV.parseVectorVal(cur_str.substring(3), Long.class);
                }

                ArrayList<VectorElem<Long>> cur_vector = GIMV.parseVectorVal(cur_str.substring(3), Long.class);
                Iterator<VectorElem<Long>> vector_iter = cur_vector.iterator();

                while (vector_iter.hasNext()) {
                    VectorElem<Long> v_elem = vector_iter.next();

                    if (out_vals[v_elem.row] == -1)
                        out_vals[v_elem.row] = v_elem.val;
                    else if (out_vals[v_elem.row] > v_elem.val)
                        out_vals[v_elem.row] = v_elem.val;
                }
                n++;
            }

            ArrayList<VectorElem<Long>> new_vector = GIMV.makeLongVectors(out_vals, block_width);
            if (self_vector == null) {
                reporter.incrCounter("ERROR", "self_vector == null", 1);
                System.err.println("ERROR: self_vector == null, key=" + key + ", # values" + n);
                return;
            }
            int isDifferent = GIMV.compareVectors(self_vector, new_vector);

            String out_prefix = "ms";
            if (isDifferent == 1)
                out_prefix += "i";    // incomplete
            else
                out_prefix += "f";    // finished

            output.collect(key, GIMV.formatVectorElemOutput(out_prefix, new_vector));
        }
    }

    //////////////////////////////////////////////////////////////////////
    // STAGE 3: Calculate number of nodes whose component id changed/unchanged.
    //  - Input: current component ids
    //  - Output: number_of_changed_nodes
    //////////////////////////////////////////////////////////////////////
    public static class MapStage3 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        // output : f n		( n : # of node whose component didn't change)
        //          i m		( m : # of node whose component changed)
        public void map(final LongWritable key, final Text value, final OutputCollector<Text, Text> output, final Reporter reporter) throws IOException {
            if (value.toString().startsWith("#"))
                return;

            final String[] line = value.toString().split("\t");
            char change_prefix = line[1].charAt(2);

            output.collect(new Text(Character.toString(change_prefix)), new Text(Integer.toString(1)));
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
    public static class MapStage4 extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
        int block_width;

        public void configure(JobConf job) {
            block_width = Integer.parseInt(job.get("block_width"));

            System.out.println("MapStage4: block_width = " + block_width);
        }

        // input sample :
        //1       msu0 1 1 1
        public void map(final LongWritable key, final Text value, final OutputCollector<LongWritable, Text> output, final Reporter reporter) throws IOException {
            final String[] line = value.toString().split("\t");
            final String[] tokens = line[1].substring(3).split(" ");
            int i;
            long block_id = Long.parseLong(line[0]);

            for (i = 0; i < tokens.length; i += 2) {
                long elem_row = Long.parseLong(tokens[i]);
                long component_id = Long.parseLong(tokens[i + 1]);

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
        FileOutputFormat.setOutputPath(conf, tempbm_path);
        FileOutputFormat.setCompressOutput(conf, true);
        FileOutputFormat.setOutputCompressorClass(conf, SnappyCodec.class);

        conf.setNumReduceTasks(nreducers);

        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(Text.class);

        return conf;
    }

    // Configure pass2
    protected JobConf configStage2() throws Exception {
        final JobConf conf = new JobConf(getConf(), ConCmptBlock.class);
        conf.set("block_width", "" + block_width);
        conf.setJobName("ConCmptBlock_pass2");

        conf.setMapperClass(MapStage2.class);
        conf.setReducerClass(RedStage2.class);

        FileInputFormat.setInputPaths(conf, tempbm_path);
        FileOutputFormat.setOutputPath(conf, nextbm_path);
        FileOutputFormat.setCompressOutput(conf, true);
        FileOutputFormat.setOutputCompressorClass(conf, SnappyCodec.class);

        conf.setNumReduceTasks(nreducers);

        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(Text.class);

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
        FileOutputFormat.setOutputCompressorClass(conf, SnappyCodec.class);

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
        FileOutputFormat.setOutputCompressorClass(conf, SnappyCodec.class);

        conf.setNumReduceTasks(nreducers);

        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(LongWritable.class);

        return conf;
    }
}

