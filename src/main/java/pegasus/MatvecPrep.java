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
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class MatvecPrep extends Configured implements Tool {

    public static class MapStage1 extends MapReduceBase implements Mapper<LongWritable, Text, BlockIndexWritable, BlockWritable> {
        int block_size;
        long matrix_row;
        int makesym;

        private final BlockIndexWritable KEY = new BlockIndexWritable();
        private final BlockWritable VALUE = new BlockWritable();

        public void configure(JobConf job) {
            block_size = Integer.parseInt(job.get("block_size"));
            matrix_row = Long.parseLong(job.get("matrix_row"));
            makesym = Integer.parseInt(job.get("makesym"));
            System.out.println("MapStage1: block_size = " + block_size + ", matrix_row=" + matrix_row + ", makesym = " + makesym);
        }

        public void map(final LongWritable key, final Text value, final OutputCollector<BlockIndexWritable, BlockWritable> output, final Reporter reporter) throws IOException {
            String line_text = value.toString();

            final String[] line = line_text.split("\t");

            if (line[1].charAt(0) == 'v') {
                long row_id = Long.parseLong(line[0]);
                long block_id = row_id / block_size;
                long in_block_index = row_id % block_size;

                VALUE.reset();
                VALUE.setTypeVector(block_size);
                VALUE.setVectorElem((short)in_block_index, Long.parseLong(line[1].substring(1)));

                KEY.setVectorIndex(block_id);
                output.collect(KEY, VALUE);
            } else {
                long row_id = Long.parseLong(line[0]);
                long col_id = Long.parseLong(line[1]);
                long block_rowid = row_id / block_size;
                long block_colid = col_id / block_size;
                short in_block_row = (short)(row_id % block_size);  // get rided of transpose ? safe ?
                short in_block_col = (short)(col_id % block_size); // get rided of transpose ? safe ?

                VALUE.reset();
                VALUE.setTypeMatrix();
                VALUE.addMatrixElem(in_block_row, in_block_col);
                KEY.setMatrixIndex(block_rowid, block_colid);

                output.collect(KEY, VALUE);
                if (makesym == 1)
                {
                    VALUE.reset();
                    VALUE.setTypeMatrix();
                    VALUE.addMatrixElem(in_block_col, in_block_row);
                    KEY.setMatrixIndex(block_colid, block_rowid);
                    output.collect(KEY, VALUE);
                }
            }
        }
    }

    public static class RedStage1 extends MapReduceBase implements Reducer<BlockIndexWritable, BlockWritable, BlockIndexWritable, BlockWritable> {
        private final BlockWritable VALUE = new BlockWritable();

        public void configure(JobConf job) {
            System.out.println("RedStage1: ");
        }

        public void reduce(final BlockIndexWritable key, final Iterator<BlockWritable> values, final OutputCollector<BlockIndexWritable, BlockWritable> output, final Reporter reporter) throws IOException {
            boolean init = false;
            while (values.hasNext()) {
                BlockWritable block = values.next();
                if (block.isTypeVector()) {
                    if (!init) {
                        VALUE.reset();
                        VALUE.setTypeVector(block.getVectorElemValues().size());
                        init = true;
                    }
                    for (int i = 0; i < block.getVectorElemValues().size(); i++) {
                        long v = block.getVectorElemValues().get(i);
                        if (v != -1L) {
                            VALUE.setVectorElem(i, v);
                        }
                    }
                }
                else {
                    if (!init) {
                        VALUE.reset();
                        VALUE.setTypeMatrix();
                        init = true;
                    }
                    for (int i = 0; i < block.getMatrixElemIndexes().size() / 2; i++) {
                        VALUE.addMatrixElem(
                                block.getMatrixElemIndexes().get(2 * i),
                                block.getMatrixElemIndexes().get(2 * i + 1));
                    }
                }
            }
            output.collect(key, VALUE);
        }
    }

    protected Path edge_path = null;
    protected Path output_path = null;
    protected long number_nodes = 0;
    protected int block_size = 1;
    protected int nreducer = 1;
    protected String output_prefix;
    protected int makesym = 0;

    public static void main(final String[] args) throws Exception {
        final int result = ToolRunner.run(new Configuration(), new MatvecPrep(), args);
        System.exit(result);
    }

    protected static int printUsage() {
        System.out.println("MatvecPrep <edge_path> <outputedge_path> <# of row> <block width> <# of reducer> <out_prefix or null> <makesym or nosym>");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }

    public int run(final String[] args) throws Exception {
        if (args.length != 7) {
            return printUsage();
        }

        edge_path = new Path(args[0]);
        output_path = new Path(args[1]);
        number_nodes = Long.parseLong(args[2]);    // number of row of matrix
        block_size = Integer.parseInt(args[3]);
        nreducer = Integer.parseInt(args[4]);

        makesym = (args[6].compareTo("makesym") == 0) ? 1 : 0;

        System.out.println("\n-----===[PEGASUS: A Peta-Scale Graph Mining System]===-----\n");
        System.out.println("[PEGASUS] Converting the adjacency matrix to block format. Output_prefix = " + output_prefix + ", makesym = " + makesym + ", block width=" + block_size + "\n");

        JobClient.runJob(configStage1());

        System.out.println("\n[PEGASUS] Conversion finished.");
        System.out.println("[PEGASUS] Block adjacency matrix is saved in the HDFS " + args[1] + "\n");
        return 0;
    }

    protected JobConf configStage1() throws Exception {
        final JobConf conf = new JobConf(getConf(), MatvecPrep.class);
        conf.set("block_size", "" + block_size);
        conf.set("matrix_row", "" + number_nodes);
        conf.set("makesym", "" + makesym);
        conf.setJobName("MatvecPrep_Stage1");

        conf.setMapperClass(MapStage1.class);
        conf.setReducerClass(RedStage1.class);

        FileSystem fs = FileSystem.get(getConf());
        fs.delete(output_path, true);

        FileInputFormat.setInputPaths(conf, edge_path);
        SequenceFileOutputFormat.setOutputPath(conf, output_path);
        SequenceFileOutputFormat.setCompressOutput(conf, true);

        conf.setOutputFormat(SequenceFileOutputFormat.class);
        conf.set("mapred.output.compression.type", "BLOCK");

        conf.setNumReduceTasks(nreducer);

        conf.setMapOutputKeyClass(BlockIndexWritable.class);
        conf.setMapOutputValueClass(BlockWritable.class);

        conf.setOutputKeyClass(BlockIndexWritable.class);
        conf.setOutputValueClass(BlockWritable.class);

        ConCmptBlock.setCompression(conf);

        return conf;
    }
}

