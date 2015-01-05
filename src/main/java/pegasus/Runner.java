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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Runner extends Configured implements Tool {

    public static int MAX_ITERATIONS = 1024;

    protected Path pathEdges = null;
    protected Path pathVector = null;
    protected Path workDir = null;
    protected Path pathOutputStage1 = null;
    protected Path pathOutputStage2 = null;
    protected Path pathOutputVector = null;
    protected int numberOfReducers = 1;
    protected int blockWidth = 64;

    public static void main(final String[] args) throws Exception {
        final int result = ToolRunner.run(new Configuration(), new Runner(), args);
        System.exit(result);
    }

    public int run(final String[] args) throws Exception {
        pathEdges = new Path(args[0]);
        pathVector = new Path(args[1]);
        workDir = new Path(args[2]);
        pathOutputStage1 = new Path(workDir, "stage1");
        pathOutputStage2 = new Path(workDir, "stage2");
        pathOutputVector = new Path(workDir, "result");
        numberOfReducers = Integer.parseInt(args[3]);

        blockWidth = Integer.parseInt(args[4]);

        System.out.println("\n-----===[PEGASUS: A Peta-Scale Graph Mining System]===-----\n");
        System.out.println("[PEGASUS] Computing connected component using block method. Reducers = " + numberOfReducers + ", block_width = " + blockWidth);

        final FileSystem fs = FileSystem.get(getConf());

        int n = 0;
        for (; n < MAX_ITERATIONS; n++) {
            Job job1 = configStage1();
            Job job2 = configStage2();

            boolean res1 = job1.waitForCompletion(true);
            boolean res2 = job2.waitForCompletion(true);

            // fixme: check return code

            long changed = job2.getCounters().findCounter(PegasusCounter.NUMBER_INCOMPLETE_VECTOR).getValue();
            long unchanged = job2.getCounters().findCounter(PegasusCounter.NUMBER_FINAL_VECTOR).getValue();

            System.out.println("Hop " + n + " : changed = " + changed + ", unchanged = " + unchanged);

            if (changed == 0) {
                System.out.println("All the component ids converged. Finishing...");
                fs.delete(pathOutputStage1, true);
                fs.delete(pathVector, true);
                fs.rename(pathOutputStage2, pathVector);
                System.out.println("Unfolding the block structure for easy lookup...");
                configStage3().waitForCompletion(true);  // fixme: check return code
                break;
            }
            fs.delete(pathOutputStage1, true);
            fs.delete(pathVector, true);
            fs.rename(pathOutputStage2, pathVector);
        }
        System.out.println("\n[PEGASUS] Connected component computed.");
        System.out.println("[PEGASUS] Total Iteration = " + n);
        System.out.println("[PEGASUS] Connected component information is saved in the HDFS concmpt_curbm as\n\"node_id	'msf'component_id\" format");
        return 0;
    }

    protected Job configStage1() throws Exception {
        Configuration conf = getConf();
        conf.set("block_width", "" + blockWidth);

        Job job = new Job(conf, "ConCmptBlock_pass1");
        job.setJarByClass(Runner.class);

        job.setMapperClass(Stage1.Mapper1.class);
        job.setReducerClass(Stage1.Reducer1.class);

        FileInputFormat.setInputPaths(job, pathEdges, pathVector);
        SequenceFileOutputFormat.setOutputPath(job, pathOutputStage1);
        SequenceFileOutputFormat.setCompressOutput(job, true);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        conf.set("mapred.output.compression.type", "BLOCK");

        job.setNumReduceTasks(numberOfReducers);

        job.setMapOutputKeyClass(Stage1.JoinKey.class);
        job.setMapOutputValueClass(BlockWritable.class);
        job.setOutputKeyClass(VLongWritable.class);
        job.setOutputValueClass(BlockWritable.class);
        job.setGroupingComparatorClass(Stage1.IndexComparator.class);
        job.setPartitionerClass(Stage1.IndexPartitioner.class);
        job.setSortComparatorClass(Stage1.SortComparator.class);

        setCompression(job);

        return job;
    }

    protected Job configStage2() throws Exception {
        Configuration conf = getConf();

        Job job = new Job(conf, "ConCmptBlock_pass2");
        job.setJarByClass(Runner.class);
        conf.set("block_width", "" + blockWidth);

        job.setMapperClass(Mapper.class);
        job.setReducerClass(Stage2.Reducer2.class);

        SequenceFileInputFormat.setInputPaths(job, pathOutputStage1);
        FileOutputFormat.setOutputPath(job, pathOutputStage2);
        FileOutputFormat.setCompressOutput(job, true);

        job.setNumReduceTasks(numberOfReducers);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(VLongWritable.class);
        job.setMapOutputValueClass(BlockWritable.class);
        job.setOutputKeyClass(BlockIndexWritable.class);
        job.setOutputValueClass(BlockWritable.class);
        job.setSortComparatorClass(VLongWritableComparator.class);

        setCompression(job);
        return job;
    }

    protected Job configStage3() throws Exception {
        Configuration conf = getConf();

        Job job = new Job(conf, "ConCmptBlock_pass4");
        job.setJarByClass(Runner.class);

        conf.set("block_width", "" + blockWidth);

        job.setMapperClass(Stage3.Mapper3.class);

        FileInputFormat.setInputPaths(job, pathVector);
        FileOutputFormat.setOutputPath(job, pathOutputVector);
        FileOutputFormat.setCompressOutput(job, true);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setNumReduceTasks(0);

        job.setOutputKeyClass(VLongWritable.class);
        job.setOutputValueClass(Text.class);

        setCompression(job);
        return job;
    }

    public static void setCompression(Job job) {
     //   FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
         FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
    }
}
