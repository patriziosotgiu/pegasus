/***********************************************************************
 PEGASUS: Peta-Scale Graph Mining System
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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import static pegasus.Utils.*;

import java.io.IOException;
import java.util.Arrays;

public class ConCmptBlock1Test {

    MapDriver<BlockIndexWritable, BlockWritable, ConCmptBlock.Stage1JoinKey, BlockWritable> mapDriver;
    ReduceDriver<ConCmptBlock.Stage1JoinKey, BlockWritable, LongWritable, BlockWritable> reduceDriver;
    MapReduceDriver<BlockIndexWritable, BlockWritable, ConCmptBlock.Stage1JoinKey, BlockWritable, LongWritable, BlockWritable> mrDriver;


    @Before
    public void setUp() {
        ConCmptBlock.MapStage1 mapper = new ConCmptBlock.MapStage1();
        mapDriver = MapDriver.newMapDriver(mapper);

        ConCmptBlock.RedStage1 reducer = new ConCmptBlock.RedStage1();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);

        mrDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
        mrDriver.setKeyGroupingComparator(new ConCmptBlock.Stage1GroupComparator());
    }

    // group by vector.row and matrix.col
    @Test
    public void map() throws IOException {
        mapDriver.addInput(blockIndex(0), blockVector(0, 11, 12, 13, 14));
        mapDriver.addInput(blockIndex(1, 0), blockMatrix(1L, 1, 2, 1, 3, 4, 5));

        mapDriver.addOutput(new ConCmptBlock.Stage1JoinKey(true, 0), blockVector(0, 11, 12, 13, 14));
        mapDriver.addOutput(new ConCmptBlock.Stage1JoinKey(false, 0), blockMatrix(1L, 1, 2, 1, 3, 4, 5));

        mapDriver.runTest();
    }

    // group by vector.row and matrix.col
    @Test
    public void map2() throws IOException {
        mapDriver.getConfiguration().setInt("block_width", 3);
        mapDriver.getConfiguration().setInt("recursive_diagmult", 0);

        int block_col = 1;

        mapDriver.addInput(blockIndex(0), blockVector(0, 1, 2));
        mapDriver.addInput(blockIndex(1, 0), blockMatrix(block_col, 0, 1, 1, 0, 1, 2, 2, 1));

        mapDriver.addOutput(new ConCmptBlock.Stage1JoinKey(true, 0), blockVector(0, 1, 2));
        mapDriver.addOutput(new ConCmptBlock.Stage1JoinKey(false, 0), blockMatrix(block_col, 0, 1, 1, 0, 1, 2, 2, 1));

        mapDriver.runTest();
    }

    //
    //     |0 1 0|      |0|        |1|
    // M = |1 0 1|  V = |1|  res = |0|
    //     |0 1 0|      |2|        |1|
    @Test
    public void reduce() throws IOException {
        reduceDriver.getConfiguration().setInt("block_width", 3);
        reduceDriver.getConfiguration().setInt("recursive_diagmult", 0);

        int block_col = 0;

        BlockWritable e1 = blockVector(0, 1, 2);
        BlockWritable e2 = blockMatrix(block_col, 0, 1, 1, 0, 1, 2, 2, 1);

        reduceDriver.addInput(new ConCmptBlock.Stage1JoinKey(true, block_col), Arrays.asList(e1, e2));

        BlockWritable v1 = blockVector(BlockWritable.TYPE.INITIAL, 0, 1, 2);
        BlockWritable v2 = blockVector(BlockWritable.TYPE.INCOMPLETE, 1, 0, 1);

        reduceDriver.addOutput(new LongWritable(block_col), v1); // initial vector
        reduceDriver.addOutput(new LongWritable(block_col), v2); // after multiplication
        reduceDriver.runTest();
    }

    //
    //     |0 1 0|      |0|        |1|
    // M = |1 0 1|  V = |1|  res = |0|
    //     |0 1 0|      |2|        |1|
    @Test
    public void mapReduce() throws IOException {
        mrDriver.getConfiguration().setInt("block_width", 3);
        mrDriver.getConfiguration().setInt("recursive_diagmult", 0);

        mrDriver.addInput(blockIndex(0), blockVector(0, 1, 2));
        mrDriver.addInput(blockIndex(0, 0), blockMatrix(1, 0, 1, 1, 0, 1, 2, 2, 1));

        mrDriver.addOutput(new LongWritable(0), blockVector(BlockWritable.TYPE.INITIAL, 0, 1, 2));
        mrDriver.addOutput(new LongWritable(0), blockVector(BlockWritable.TYPE.INCOMPLETE, 1, 0, 1));
        mrDriver.runTest();
    }

    //
    //     |0 0 0|      | 1|        |-1|
    // M = |0 0 0|  V = |-1|  res = |-1|
    //     |1 0 0|      |-1|        | 1|
    @Test
    public void reduce2() throws IOException {
        reduceDriver.getConfiguration().setInt("block_width", 3);
        reduceDriver.getConfiguration().setInt("recursive_diagmult", 1);

        int block_col = 0;

        BlockWritable e1 = blockVector(1, -1, -1);
        BlockWritable e2 = blockMatrix(block_col, 2, 0);

        reduceDriver.addInput(new ConCmptBlock.Stage1JoinKey(true, block_col), Arrays.asList(e1, e2));

        BlockWritable v1 = blockVector(BlockWritable.TYPE.INITIAL, 1, -1, -1);
        BlockWritable v2 = blockVector(BlockWritable.TYPE.INCOMPLETE, -1, -1, 1);

        reduceDriver.addOutput(new LongWritable(block_col), v1); // initial vector
        reduceDriver.addOutput(new LongWritable(block_col), v2); // after multiplication
        reduceDriver.runTest();
    }

    //
    //     |0 1 0 0|      |0|        |0|
    // M = |1 0 1 0|  V = |1|  res = |0|
    //     |0 1 0 0|      |2|        |1|
    //     |0 0 0 1|      |3|        |3|
    //
    // M = |A B|      V = |X|  res = |AX| + |BY|
    //     |C D|          |Y|        |CX|   |DY|
    //
    //  |0 1|   |0|   |0|
    //  |1 0| x |1| = |0|   AX  (row 0)
    //
    //  |0 0|   |2|   |-1|
    //  |1 0| x |3| = |2|   BY  (row 0)
    //
    //  |0 1|   |0|   | 0|
    //  |0 0| x |1| = |-1|   CX  (row 1)
    //
    //  |0 0|   |2|   |-1|
    //  |0 1| x |3| = | 3|   DY  (row 1)
    //

    @Test
    public void mapReduce2() throws IOException {
        mrDriver.getConfiguration().setInt("block_width", 2);
        mrDriver.getConfiguration().setInt("recursive_diagmult", 0);

        mrDriver.addInput(blockIndex(0), blockVector(0, 1));
        mrDriver.addInput(blockIndex(1), blockVector(2, 3));

        mrDriver.addInput(blockIndex(0, 0), blockMatrix(0L, 0, 1, 1, 0));
        mrDriver.addInput(blockIndex(0, 1), blockMatrix(1L, 1, 0));
        mrDriver.addInput(blockIndex(1, 0), blockMatrix(0L, 0, 1));
        mrDriver.addInput(blockIndex(1, 1), blockMatrix(1L, 1, 1));

        mrDriver.addOutput(new LongWritable(0), blockVector(BlockWritable.TYPE.INITIAL, 0, 1));
        mrDriver.addOutput(new LongWritable(0), blockVector(BlockWritable.TYPE.INCOMPLETE, 1, 0));
        mrDriver.addOutput(new LongWritable(1), blockVector(BlockWritable.TYPE.INCOMPLETE, 1, -1));
        mrDriver.addOutput(new LongWritable(1), blockVector(BlockWritable.TYPE.INITIAL, 2, 3));
        mrDriver.addOutput(new LongWritable(0), blockVector(BlockWritable.TYPE.INCOMPLETE, -1, 2));
        mrDriver.addOutput(new LongWritable(1), blockVector(BlockWritable.TYPE.INCOMPLETE, -1, 3));

        mrDriver.runTest();
    }

}

