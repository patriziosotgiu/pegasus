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

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

public class ConCmptIVGen extends Configured implements Tool {

    public static class MapStage1 extends Mapper<LongWritable, Text, VLongWritable, Text> {
        @Override
        public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line_text = value.toString();
            String[] line = line_text.split("\t");
            if (line.length < 3)
                return;
            ctx.write(new VLongWritable(Long.parseLong(line[0])), new Text(line[1] + "\t" + line[2]));
        }
    }

    public static class RedStage1 extends Reducer<VLongWritable, Text, VLongWritable, Text> {
        long number_nodes = 0;
        private final VLongWritable KEY = new VLongWritable();
        private final Text VALUE = new Text();

        @Override
        public void setup(Context ctx) {
            Configuration conf = ctx.getConfiguration();
            number_nodes = Long.parseLong(conf.get("number_nodes"));
            System.out.println("Reducer1: number_nodes = " + number_nodes);
        }

        @Override
        public void reduce(VLongWritable key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            long start_node;
            long end_node;

            for (Text value : values) {
                String[] line = value.toString().split("\t");

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

    //////////////////////////////////////////////////////////////////////
    // command line interface
    //////////////////////////////////////////////////////////////////////
    protected Path pathBitmask = null;
    protected Path pathVector = null;
    protected long number_nodes = 0;
    protected int number_reducers = 1;
    FileSystem fs;

    public static void main(final String[] args) throws Exception {
        final int result = ToolRunner.run(new Configuration(), new ConCmptIVGen(), args);
        System.exit(result);
    }

    public int run(final String[] args) throws Exception {
        number_nodes = Long.parseLong(args[1]);
        number_reducers = Integer.parseInt(args[2]);
        pathBitmask = new Path(args[0] + ".TMP");
        pathVector = new Path(args[0]);

        System.out.println("\n-----===[PEGASUS: A Peta-Scale Graph Mining System]===-----\n");
        System.out.println("[PEGASUS] Generating initial vector. Output path = " + args[0] + ", Number of nodes = " + number_nodes + ", Number of machines =" + number_reducers + "\n");

        gen_cmd_file(number_nodes, number_reducers, pathBitmask);

        int res = configStage1().waitForCompletion(true) ? 0 : 1;
        FileSystem.get(getConf()).delete(pathBitmask);

        System.out.println("\n[PEGASUS] Initial connected component vector generated in HDFS " + args[0] + "\n");

        return res;
    }

    // generate bitmask command file which is used in the 1st iteration.
    public void gen_cmd_file(long num_nodes, int num_reducers, Path output_path) throws IOException {
        File tmpFile = File.createTempFile("pegasus_initial_vector", "");
        tmpFile.deleteOnExit();

        FileWriter file = new FileWriter(tmpFile);
        BufferedWriter out = new BufferedWriter(file);

        System.out.print("creating initial vector generation cmd...");

        long step = num_nodes / num_reducers;
        long start_node, end_node;

        for (int i = 0; i < num_reducers; i++) {
            start_node = i * step;
            if (i < num_reducers - 1)
                end_node = step * (i + 1) - 1;
            else
                end_node = num_nodes - 1;
            out.write(i + "\t" + start_node + "\t" + end_node + "\n");
        }
        out.close();
        System.out.println("done.");

        final FileSystem fs = FileSystem.get(getConf());
        fs.copyFromLocalFile(true, new Path(tmpFile.getAbsolutePath()), output_path);
    }

    protected Job configStage1() throws Exception {
        Configuration conf = getConf();
        conf.set("number_nodes", "" + number_nodes);

        Job job = new Job(conf, "ConCmptIVGen");
        job.setJarByClass(ConCmptIVGen.class);

        job.setMapperClass(MapStage1.class);
        job.setReducerClass(RedStage1.class);

        FileInputFormat.setInputPaths(job, pathBitmask);
        FileOutputFormat.setOutputPath(job, pathVector);
        FileOutputFormat.setCompressOutput(job, true);
//        FileOutputFormat.setOutputCompressorClass(conf, SnappyCodec.class);

        job.setNumReduceTasks(number_reducers);

        job.setOutputKeyClass(VLongWritable.class);
        job.setOutputValueClass(Text.class);

        return job;
    }
}

