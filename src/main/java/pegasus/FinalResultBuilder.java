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
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FinalResultBuilder {

    public static class _Mapper extends Mapper<BlockIndexWritable, BlockWritable, VLongWritable, Text> {
        private final VLongWritable KEY   = new VLongWritable();
        private final Text          VALUE = new Text();

        private int blockWidth = 32;

        @Override
        public void setup(Mapper.Context ctx) {
            Configuration conf = ctx.getConfiguration();
            blockWidth = conf.getInt("blockWidth", 32);
        }

        public void map(BlockIndexWritable key, BlockWritable value, Context ctx) throws IOException, InterruptedException {
            long blockIdx = key.getI();
            for (int i = 0; i < value.getVectorElemValues().size(); i++) {
                long component = value.getVectorElemValues().get(i);
                if (component >= 0) {
                    KEY.set(blockWidth * blockIdx + i);
                    VALUE.set("msf" + component);
                    ctx.write(KEY, VALUE);
                }
            }
        }
    }
}
