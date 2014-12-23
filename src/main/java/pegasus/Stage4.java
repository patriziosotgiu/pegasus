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

import com.google.common.base.Objects;
import gnu.trove.list.array.TLongArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

public class Stage4 {

    public static class Mapper4 extends MapReduceBase implements Mapper<BlockIndexWritable, BlockWritable, LongWritable, LongWritable> {
        private final LongWritable KEY = new LongWritable();
        private final LongWritable VALUE = new LongWritable(1);
        int blockWidth;

        public void configure(JobConf job) {
            blockWidth = Integer.parseInt(job.get("block_width"));
            System.out.println("Mapper4 : configure is called.  block_width=" + blockWidth);
        }

        public void map(final BlockIndexWritable key, final BlockWritable value, final OutputCollector<LongWritable, LongWritable> output, final Reporter reporter) throws IOException {
            for (int i = 0; i < value.getVectorElemValues().size(); i ++) {
                KEY.set(value.getVectorElemValues().get(i));
                output.collect(KEY, VALUE);
            }
        }
    }

    public static class Reducer4 extends MapReduceBase implements Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
        public void reduce(final LongWritable key, final Iterator<LongWritable> values, OutputCollector<LongWritable, LongWritable> output, final Reporter reporter) throws IOException {
            int count = 0;
            while (values.hasNext()) {
                count += values.next().get();
            }
            output.collect(key, new LongWritable(count));
        }
    }
}
