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

    private Path pathEdges        = null;
    private Path pathVector       = null;
    private Path workDir          = null;
    private Path pathOutputStage1 = null;
    private Path pathOutputStage2 = null;
    private Path pathOutputVector = null;
    private int  numberOfReducers = 1;
    private int  blockWidth       = 64;

    public static void main(final String[] args) throws Exception {
        final int result = ToolRunner.run(new Configuration(), new Runner(), args);
        System.exit(result);
    }

    public int run(final String[] args) throws Exception {
        pathEdges        = new Path(args[0]);
        pathVector       = new Path(args[1]);
        workDir          = new Path(args[2]);
        pathOutputStage1 = new Path(workDir, "stage1");
        pathOutputStage2 = new Path(workDir, "stage2");
        pathOutputVector = new Path(workDir, "result");
        numberOfReducers = Integer.parseInt(args[3]);
        blockWidth       = Integer.parseInt(args[4]);

        FileSystem fs = FileSystem.get(getConf());

        Job job1 = buildJob1();
        Job job2 = buildJob2();
        Job job3 = buildJob3();

        int n = 0;
        for (; n < MAX_ITERATIONS; n++) {
            if (!job1.waitForCompletion(true)) {
                System.err.println("Failed to execute IterationStage1 for iteration #" + n);
                return -1;
            }
            if (!job2.waitForCompletion(true)) {
                System.err.println("Failed to execute IterationStage2 for iteration #" + n);
                return -1;
            }

            long changed = job2.getCounters().findCounter(PegasusCounter.NUMBER_INCOMPLETE_VECTOR).getValue();
            long unchanged = job2.getCounters().findCounter(PegasusCounter.NUMBER_FINAL_VECTOR).getValue();
            System.out.println("Iteration #" + n + ", changed=" + changed + ", unchanged=" + unchanged);

            fs.delete(pathOutputStage1, true);
            fs.delete(pathVector, true);
            fs.rename(pathOutputStage2, pathVector);

            if (changed == 0) {
                if (!job3.waitForCompletion(true)) {
                    System.err.println("Failed to execute FinalResultBuilder for iteration #" + n);
                    return -1;
                }
                break;
            }
        }
        System.out.println("Connected component computed in " + n + " iterations");
        return 0;
    }

    private Job buildJob1() throws Exception {
        Configuration conf = getConf();
        conf.setInt("blockWidth", blockWidth);
        conf.set("mapred.output.compression.type", "BLOCK");

        Job job = new Job(conf, "ConCmptBlock_pass1");
        job.setJarByClass(Runner.class);

        job.setMapperClass(IterationStage1._Mapper.class);
        job.setReducerClass(IterationStage1._Reducer.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setNumReduceTasks(numberOfReducers);
        job.setMapOutputKeyClass(IterationStage1.JoinKey.class);
        job.setMapOutputValueClass(BlockWritable.class);
        job.setOutputKeyClass(VLongWritable.class);
        job.setOutputValueClass(BlockWritable.class);
        job.setGroupingComparatorClass(IterationStage1.IndexComparator.class);
        job.setPartitionerClass(IterationStage1.IndexPartitioner.class);
        job.setSortComparatorClass(IterationStage1.SortComparator.class);

        FileInputFormat.setInputPaths(job, pathEdges, pathVector);
        SequenceFileOutputFormat.setOutputPath(job, pathOutputStage1);
        SequenceFileOutputFormat.setCompressOutput(job, true);

        setCompression(job);

        return job;
    }

    private Job buildJob2() throws Exception {
        Configuration conf = getConf();
        conf.setInt("blockWidth", blockWidth);

        Job job = new Job(conf, "ConCmptBlock_pass2");
        job.setJarByClass(Runner.class);

        job.setMapperClass(Mapper.class);
        job.setReducerClass(IterationStage2._Reducer.class);
        job.setNumReduceTasks(numberOfReducers);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setMapOutputKeyClass(VLongWritable.class);
        job.setMapOutputValueClass(BlockWritable.class);
        job.setOutputKeyClass(BlockIndexWritable.class);
        job.setOutputValueClass(BlockWritable.class);
        job.setSortComparatorClass(VLongWritableComparator.class);

        SequenceFileInputFormat.setInputPaths(job, pathOutputStage1);
        FileOutputFormat.setOutputPath(job, pathOutputStage2);
        FileOutputFormat.setCompressOutput(job, true);

        setCompression(job);
        return job;
    }

    private Job buildJob3() throws Exception {
        Configuration conf = getConf();
        conf.setInt("blockWidth", blockWidth);

        Job job = new Job(conf, "ConCmptBlock_pass4");
        job.setJarByClass(Runner.class);

        job.setMapperClass(FinalResultBuilder._Mapper.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(VLongWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, pathVector);
        FileOutputFormat.setOutputPath(job, pathOutputVector);
        FileOutputFormat.setCompressOutput(job, true);

        setCompression(job);
        return job;
    }

    public static void setCompression(Job job) {
     //   FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
         FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
    }
}