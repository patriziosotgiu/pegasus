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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class BlocksBuilderTest {
    MapDriver<LongWritable, Text, BlockIndexWritable, BlocksBuilder.LightBlockWritable> mapDriver;
    ReduceDriver<BlockIndexWritable, BlocksBuilder.LightBlockWritable, BlockIndexWritable, BlockWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, BlockIndexWritable, BlocksBuilder.LightBlockWritable, BlockIndexWritable, BlockWritable> mrDriver;

    @Before
    public void setUp() {
        BlocksBuilder.MapStage1 mapper = new BlocksBuilder.MapStage1();
        mapDriver = MapDriver.newMapDriver(mapper);
        BlocksBuilder.RedStage1 reducer = new BlocksBuilder.RedStage1();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mrDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    //
    //   0 1 2 | 3 4
    // 0       |
    // 1     x | x
    // 2   x   |
    // --------+------
    // 3   x   |   x
    // 4       | x
    @Test
    public void simpleMatrix() throws IOException {
        mrDriver.getConfiguration().setInt(Constants.PROP_BLOCK_SIZE, 3);
        mrDriver.getConfiguration().setBoolean("isVector", false);

        mrDriver.addInput(new LongWritable(0), new Text("1\t2"));
        mrDriver.addInput(new LongWritable(0), new Text("1\t3"));
        mrDriver.addInput(new LongWritable(0), new Text("3\t4"));

        BlockIndexWritable b1 = new BlockIndexWritable();
        BlockIndexWritable b2 = new BlockIndexWritable();
        BlockIndexWritable b3 = new BlockIndexWritable();
        BlockIndexWritable b4 = new BlockIndexWritable();

        BlockWritable d1 = new BlockWritable(3, BlockWritable.TYPE.MATRIX);
        BlockWritable d2 = new BlockWritable(3, BlockWritable.TYPE.MATRIX);
        BlockWritable d3 = new BlockWritable(3, BlockWritable.TYPE.MATRIX);
        BlockWritable d4 = new BlockWritable(3, BlockWritable.TYPE.MATRIX);


        b1.setMatrixIndex(0, 0);
        d1.addMatrixElem(1, 2);
        d1.addMatrixElem(2, 1);

        b2.setMatrixIndex(0, 1);
        d2.addMatrixElem(1, 0);

        b3.setMatrixIndex(1, 0);
        d3.addMatrixElem(0, 1);

        b4.setMatrixIndex(1, 1);
        d4.addMatrixElem(0, 1);
        d4.addMatrixElem(1, 0);

        mrDriver.addOutput(b1, d1);
        mrDriver.addOutput(b2, d2);
        mrDriver.addOutput(b3, d3);
        mrDriver.addOutput(b4, d4);

        mrDriver.runTest();
    }

    @Test
    public void simpleVector() throws IOException {
        mrDriver.getConfiguration().setInt(Constants.PROP_BLOCK_SIZE, 3);
        mrDriver.getConfiguration().setBoolean("isVector", true);

        mrDriver.addInput(new LongWritable(0), new Text("1\t1"));
        mrDriver.addInput(new LongWritable(0), new Text("2\t2"));
        mrDriver.addInput(new LongWritable(0), new Text("3\t3"));
        mrDriver.addInput(new LongWritable(0), new Text("4\t4"));
        mrDriver.addInput(new LongWritable(0), new Text("5\t5"));
        mrDriver.addInput(new LongWritable(0), new Text("6\t6"));

        BlockIndexWritable b1 = new BlockIndexWritable();
        BlockIndexWritable b2 = new BlockIndexWritable();
        BlockIndexWritable b3 = new BlockIndexWritable();

        BlockWritable d1 = new BlockWritable(3, BlockWritable.TYPE.VECTOR_INITIAL);
        BlockWritable d2 = new BlockWritable(3, BlockWritable.TYPE.VECTOR_INITIAL);
        BlockWritable d3 = new BlockWritable(3, BlockWritable.TYPE.VECTOR_INITIAL);

        b1.setVectorIndex(0);
        d1.setVectorElem(0, -1);
        d1.setVectorElem(1, 1);
        d1.setVectorElem(2, 2);

        b2.setVectorIndex(1);
        d2.setVectorElem(0, 3);
        d2.setVectorElem(1, 4);
        d2.setVectorElem(2, 5);

        b3.setVectorIndex(2);
        d3.setVectorElem(0, 6);
        d3.setVectorElem(1, -1);
        d3.setVectorElem(2, -1);

        mrDriver.addOutput(b1, d1);
        mrDriver.addOutput(b2, d2);
        mrDriver.addOutput(b3, d3);

        mrDriver.runTest();
    }
}

