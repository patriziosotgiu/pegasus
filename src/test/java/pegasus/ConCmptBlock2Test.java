package pegasus;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import static pegasus.Utils.*;

import pegasus.BlockWritable.TYPE;


public class ConCmptBlock2Test {
    ReduceDriver<LongWritable, BlockWritable, BlockIndexWritable, BlockWritable> reduceDriver;

    @Before
    public void setUp() {
        ConCmptBlock.RedStage2 reducer = new ConCmptBlock.RedStage2();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }


    //
    //     |A B|          |X|          |AX|   |BY|
    // M = |C D|      V = |Y|    M*V = |CX| + |DY|
    //
    //      |0|
    //  V = |1|
    //      |2|
    //      |3|
    //
    //       |0|        |2|
    //  AX = |0|   BY = |2|
    //
    //       |0|        |2|
    //  CX = |1|   DY = |3|
    //
    @Test
    public void mapReduce2() throws IOException {
        reduceDriver.getConfiguration().setInt("block_width", 2);
        reduceDriver.getConfiguration().setInt("recursive_diagmult", 0);

        reduceDriver.addInput(new LongWritable(0), Arrays.asList(
                blockVector(TYPE.INITIAL, 0, 1),
                blockVector(TYPE.INCOMPLETE, 0, 0),
                blockVector(TYPE.INCOMPLETE, 2, 2)));

        reduceDriver.addInput(new LongWritable(1), Arrays.asList(
                blockVector(TYPE.INITIAL, 2, 3),
                blockVector(TYPE.INCOMPLETE, 0, 1),
                blockVector(TYPE.INCOMPLETE, 2, 3)));

        reduceDriver.addOutput(blockIndex(0), blockVector(TYPE.INCOMPLETE, 0, 0));
        reduceDriver.addOutput(blockIndex(1), blockVector(TYPE.INCOMPLETE, 0, 1));

        reduceDriver.runTest();
    }
}

