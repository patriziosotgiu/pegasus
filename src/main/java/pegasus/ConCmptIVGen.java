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
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class ConCmptIVGen extends Configured implements Tool {

    public static class MapStage1 extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
        public void map(final LongWritable key, final Text value, final OutputCollector<LongWritable, Text> output, final Reporter reporter) throws IOException {
            String line_text = value.toString();
            String[] line = line_text.split("\t");
            if (line.length < 3)
                return;
            output.collect(new LongWritable(Long.parseLong(line[0])), new Text(line[1] + "\t" + line[2]));
        }
    }

    public static class RedStage1 extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {
        long number_nodes = 0;
        private final LongWritable KEY = new LongWritable();
        private final Text VALUE = new Text();

        public void configure(JobConf job) {
            number_nodes = Long.parseLong(job.get("number_nodes"));
            System.out.println("Reducer1: number_nodes = " + number_nodes);
        }

        public void reduce(final LongWritable key, final Iterator<Text> values, OutputCollector<LongWritable, Text> output, final Reporter reporter) throws IOException {
            long start_node;
            long end_node;

            while (values.hasNext()) {
                String[] line = values.next().toString().split("\t");

                start_node = Long.parseLong(line[0]);
                end_node = Long.parseLong(line[1]);

                for (long i = start_node; i <= end_node; i++) {
                    KEY.set(i);
                    VALUE.set(Long.toString(i));
                    output.collect(KEY, VALUE);
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

        JobClient.runJob(configStage1());
        FileSystem.get(getConf()).delete(pathBitmask);

        System.out.println("\n[PEGASUS] Initial connected component vector generated in HDFS " + args[0] + "\n");

        return 0;
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

    protected JobConf configStage1() throws Exception {
        final JobConf conf = new JobConf(getConf(), ConCmptIVGen.class);
        conf.set("number_nodes", "" + number_nodes);
        conf.setJobName("ConCmptIVGen_Stage1");

        conf.setMapperClass(MapStage1.class);
        conf.setReducerClass(RedStage1.class);

        FileInputFormat.setInputPaths(conf, pathBitmask);
        FileOutputFormat.setOutputPath(conf, pathVector);
        FileOutputFormat.setCompressOutput(conf, true);
//        FileOutputFormat.setOutputCompressorClass(conf, SnappyCodec.class);

        conf.setNumReduceTasks(number_reducers);

        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(Text.class);

        return conf;
    }
}

