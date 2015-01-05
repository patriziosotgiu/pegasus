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

import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class Stage3 {

    public static class Mapper3 extends MapReduceBase implements Mapper<BlockIndexWritable, BlockWritable, VLongWritable, Text> {
        int blockWidth;

        public void configure(JobConf job) {
            blockWidth = Integer.parseInt(job.get("block_width"));
            System.out.println("Mapper3: block_width = " + blockWidth);
        }

        public void map(final BlockIndexWritable key, final BlockWritable value, final OutputCollector<VLongWritable, Text> output, final Reporter reporter) throws IOException {
            long block_id = key.getI();
            for (int i = 0; i < value.getVectorElemValues().size(); i++) {
                long component_id = value.getVectorElemValues().get(i);
                if (component_id >= 0) {
                    output.collect(new VLongWritable(blockWidth * block_id + i), new Text("msf" + component_id));
                }
            }
        }
    }
}
