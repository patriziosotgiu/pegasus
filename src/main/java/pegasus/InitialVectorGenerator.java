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

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

public class InitialVectorGenerator extends Configured implements Tool {

    public static class _Mapper extends Mapper<LongWritable, Text, VLongWritable, Text> {
        private final VLongWritable KEY   = new VLongWritable();
        private final Text          VALUE = new Text();

        @Override
        public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line_text = value.toString();
            String[] line = line_text.split("\t");
            KEY.set(Long.parseLong(line[0]));
            VALUE.set(line[1] + "\t" + line[2]);
            ctx.write(KEY, VALUE);
        }
    }

    public static class _Reducer extends Reducer<VLongWritable, Text, VLongWritable, Text> {
        private final VLongWritable KEY   = new VLongWritable();
        private final Text          VALUE = new Text();

        @Override
        public void reduce(VLongWritable key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            long     start_node;
            long     end_node;
            String[] line;

            for (Text value : values) {
                line = value.toString().split("\t");
                start_node = Long.parseLong(line[0]);
                end_node = Long.parseLong(line[1]);
                for (long i = start_node; i <= end_node; i++) {
                    KEY.set(i);
                    VALUE.set(Long.toString(i));
                    ctx.write(KEY, VALUE);
                }
            }
        }
    }

    private Path pathBitmask      = null;
    private Path pathVector       = null;
    private long numberOfNodes    = 0;
    private int  numberOfReducers = 1;

    public static void main(final String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new InitialVectorGenerator(), args);
        System.exit(result);
    }

    public int run(final String[] args) throws Exception {
        numberOfNodes = Long.parseLong(args[1]);
        numberOfReducers = Integer.parseInt(args[2]);
        pathBitmask = new Path(args[0] + ".TMP");
        pathVector = new Path(args[0]);

        genCmdFile(numberOfNodes, numberOfReducers, pathBitmask);
        if (!buildJob().waitForCompletion(true)) {
            System.err.println("Failed to execute InitialVectorGenerator");
            return -1;
        }
        FileSystem.get(getConf()).delete(pathBitmask, false);
        return 0;
    }

    // generate bitmask command file which is used in the 1st iteration.
    private void genCmdFile(long numberOfNodes, int numberOfReducers, Path outputPath) throws IOException {
        File tmpFile = File.createTempFile("pegasus_initial_vector", "");
        tmpFile.deleteOnExit();
        try (BufferedWriter out = new BufferedWriter(new FileWriter(tmpFile))) {
            long step = numberOfNodes / numberOfReducers;
            long startNode;
            long endNode;
            for (int i = 0; i < numberOfReducers; i++) {
                startNode = i * step;
                if (i < numberOfReducers - 1) {
                    endNode = step * (i + 1) - 1;
                }
                else {
                    endNode = numberOfNodes - 1;
                }
                out.write(i + "\t" + startNode + "\t" + endNode + "\n");
            }
        }
        FileSystem fs = FileSystem.get(getConf());
        fs.copyFromLocalFile(true, new Path(tmpFile.getAbsolutePath()), outputPath);
    }

    private Job buildJob() throws Exception {
        Configuration conf = getConf();
        conf.setLong("numberOfNodes", numberOfNodes);

        Job job = new Job(conf, "data-piqid.pegasus.ConCmptIVGen_Stage1");
        job.setJarByClass(InitialVectorGenerator.class);
        job.setMapperClass(_Mapper.class);
        job.setReducerClass(_Reducer.class);
        job.setNumReduceTasks(numberOfReducers);
        job.setOutputKeyClass(VLongWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, pathBitmask);
        FileOutputFormat.setOutputPath(job, pathVector);
        FileOutputFormat.setCompressOutput(job, true);

        return job;
    }
}

