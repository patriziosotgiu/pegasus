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
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityMapper;
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
            JobClient.runJob(configStage1());
            RunningJob j = JobClient.runJob(configStage2());

            long changed = j.getCounters().findCounter("change", "incomplete").getValue();
            long unchanged = j.getCounters().findCounter("change", "final").getValue();

            System.out.println("Hop " + n + " : changed = " + changed + ", unchanged = " + unchanged);

            if (changed == 0) {
                System.out.println("All the component ids converged. Finishing...");
                fs.delete(pathOutputStage1, true);
                fs.delete(pathVector, true);
                fs.rename(pathOutputStage2, pathVector);
                System.out.println("Unfolding the block structure for easy lookup...");
                JobClient.runJob(configStage3());
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

    protected JobConf configStage1() throws Exception {
        final JobConf conf = new JobConf(getConf(), Runner.class);
        conf.set("block_width", "" + blockWidth);
        conf.setJobName("ConCmptBlock_pass1");

        conf.setMapperClass(Stage1.Mapper1.class);
        conf.setReducerClass(Stage1.Reducer1.class);

        FileInputFormat.setInputPaths(conf, pathEdges, pathVector);
        SequenceFileOutputFormat.setOutputPath(conf, pathOutputStage1);
        SequenceFileOutputFormat.setCompressOutput(conf, true);

        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        conf.set("mapred.output.compression.type", "BLOCK");

        conf.setNumReduceTasks(numberOfReducers);

        conf.setMapOutputKeyClass(Stage1.JoinKey.class);
        conf.setMapOutputValueClass(BlockWritable.class);
        conf.setOutputKeyClass(VLongWritable.class);
        conf.setOutputValueClass(BlockWritable.class);
        conf.setOutputValueGroupingComparator(Stage1.IndexComparator.class);
        conf.setPartitionerClass(Stage1.IndexPartitioner.class);
        conf.setOutputKeyComparatorClass(Stage1.SortComparator.class);

        setCompression(conf);

        return conf;
    }

    protected JobConf configStage2() throws Exception {
        final JobConf conf = new JobConf(getConf(), Runner.class);
        conf.set("block_width", "" + blockWidth);
        conf.setJobName("ConCmptBlock_pass2");

        conf.setMapperClass(IdentityMapper.class);
        conf.setReducerClass(Stage2.Reducer2.class);

        SequenceFileInputFormat.setInputPaths(conf, pathOutputStage1);
        FileOutputFormat.setOutputPath(conf, pathOutputStage2);
        FileOutputFormat.setCompressOutput(conf, true);

        conf.setNumReduceTasks(numberOfReducers);

        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);

        conf.setMapOutputKeyClass(VLongWritable.class);
        conf.setMapOutputValueClass(BlockWritable.class);
        conf.setOutputKeyClass(BlockIndexWritable.class);
        conf.setOutputValueClass(BlockWritable.class);

        setCompression(conf);
        return conf;
    }

    protected JobConf configStage3() throws Exception {
        final JobConf conf = new JobConf(getConf(), Runner.class);
        conf.set("block_width", "" + blockWidth);
        conf.setJobName("ConCmptBlock_pass4");

        conf.setMapperClass(Stage3.Mapper3.class);

        FileInputFormat.setInputPaths(conf, pathVector);
        FileOutputFormat.setOutputPath(conf, pathOutputVector);
        FileOutputFormat.setCompressOutput(conf, true);

        conf.setInputFormat(SequenceFileInputFormat.class);

        conf.setNumReduceTasks(0);

        conf.setOutputKeyClass(VLongWritable.class);
        conf.setOutputValueClass(Text.class);

        setCompression(conf);
        return conf;
    }

    public static void setCompression(JobConf conf) {
     //   FileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
         FileOutputFormat.setOutputCompressorClass(conf, SnappyCodec.class);
    }
}
