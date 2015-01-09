/**
 * PIQASUS: Connected-component analysis for Big Graph
 *
 * __________.___________      _____    _____________ ___  _________
 * \______   \   \_____  \    /  _  \  /   _____/    |   \/   _____/
 *  |     ___/   |/  / \  \  /  /_\  \ \_____  \|    |   /\_____  \
 *  |    |   |   /   \_/.  \/    |    \/        \    |  / /        \
 *  |____|   |___\_____\ \_/\____|__  /_______  /______/ /_______  /
 *                      \__>        \/        \/                 \/
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
 *
 * ----------------------------------------------------------------------------
 * Author: Jerome Serrano <jerome.serrano@placeiq.com>
 * Date: 2015-01-09
 * ---------------------------------------------------------------------------*/

package com.placeiq.piqasus;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class Runner extends Configured implements Tool {

    public static int MAX_ITERATIONS = 1024;

    private static final Logger LOG = LogManager.getLogger(Runner.class);

    private int numberOfReducers = 1;
    private int blockSize = 64;

    public static void main(final String[] args) throws Exception {
        final int result = ToolRunner.run(new Configuration(), new Runner(), args);
        System.exit(result);
    }

    public int run(final String[] args) throws Exception {
        Path pathEdges        = new Path(args[0]);
        Path pathVector       = new Path(args[1]);
        Path workDir          = new Path(args[2]);
        Path pathOutputStage1 = new Path(workDir, "stage1");
        Path pathOutputStage2 = new Path(workDir, "stage2");
        Path pathOutputVector = new Path(workDir, "result");

        numberOfReducers = Integer.parseInt(args[3]);
        blockSize = Integer.parseInt(args[4]);

        int maxConvergence = Integer.parseInt(args[5]);
        int maxIters = Integer.parseInt(args[6]);

        if (maxConvergence < 0) {
            maxConvergence = 0;
        }
        if (maxIters < 0 || maxIters > MAX_ITERATIONS) {
             maxIters = MAX_ITERATIONS;
        }

        FileSystem fs = FileSystem.get(getConf());

        int n = 0;
        long changedNodes = Long.MAX_VALUE;
        while (n < maxIters && changedNodes > maxConvergence) {
            fs.delete(pathOutputStage1, true);
            fs.delete(pathOutputStage2, true);
            LOG.info("Start iteration " + n + " Stage1");
            Job job1 = buildJob1(pathEdges, pathVector, pathOutputStage1);
            if (!job1.waitForCompletion(true)) {
                LOG.error("Failed to execute IterationStage1 for iteration #" + n);
                return -1;
            }
            LOG.info("Start iteration " + n + " Stage2");
            Job job2 = buildJob2(pathOutputStage1, pathOutputStage2);
            if (!job2.waitForCompletion(true)) {
                LOG.error("Failed to execute IterationStage2 for iteration #" + n);
                return -1;
            }
            changedNodes = job2.getCounters().findCounter(PiqasusCounter.NUMBER_INCOMPLETE_VECTOR).getValue();
            long unchangedNodes = job2.getCounters().findCounter(PiqasusCounter.NUMBER_FINAL_VECTOR).getValue();
            LOG.info("End of iteration " + n + ", changedNodes=" + changedNodes + ", unchangedNodes=" + unchangedNodes);
            LOG.info(pathOutputStage2);
            fs.delete(pathVector, true);
            if (!fs.rename(pathOutputStage2, pathVector)) {
                LOG.error("failed to rename " + pathOutputStage2 + " into " + pathVector);
                return -1;
            }
            n++;
        }
        Job job3 = buildJob3(pathVector, pathOutputVector);
        if (!job3.waitForCompletion(true)) {
            LOG.error("Failed to execute FinalResultBuilder for iteration #" + n);
            return -1;
        }
        LOG.info("Connected component computed in " + n + " iterations");
        return 0;
    }

    private Job buildJob1(Path input1, Path input2, Path output) throws Exception {
        Configuration conf = getConf();
        conf.setInt(Constants.PROP_BLOCK_SIZE, blockSize);
        conf.set("mapred.output.compression.type", "BLOCK");

        Job job = new Job(conf, "data-piqid.piqasus.IterationStage1");
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

        FileInputFormat.setInputPaths(job, input1, input2);
        SequenceFileOutputFormat.setOutputPath(job, output);
        SequenceFileOutputFormat.setCompressOutput(job, true);

        setCompression(job);

        return job;
    }

    private Job buildJob2(Path input, Path output) throws Exception {
        Configuration conf = getConf();
        conf.setInt(Constants.PROP_BLOCK_SIZE, blockSize);

        Job job = new Job(conf, "data-piqid.piqasus.IterationStage2");
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

        SequenceFileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);
        FileOutputFormat.setCompressOutput(job, true);

        setCompression(job);
        return job;
    }

    private Job buildJob3(Path input, Path output) throws Exception {
        Configuration conf = getConf();
        conf.setInt(Constants.PROP_BLOCK_SIZE, blockSize);

        Job job = new Job(conf, "data-piqid.piqasus.FinalResultBuilder");
        job.setJarByClass(Runner.class);

        job.setMapperClass(FinalResultBuilder._Mapper.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(VLongWritable.class);
        job.setOutputValueClass(VLongWritable.class);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);
        FileOutputFormat.setCompressOutput(job, true);

        setCompression(job);
        return job;
    }

    public static void setCompression(Job job) {
        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        job.getConfiguration().set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
    }
}