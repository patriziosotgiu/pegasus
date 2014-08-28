package pegasus;

import gnu.trove.list.array.TLongArrayList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class ConCmptBlockTest {

    MapDriver<BlockIndexWritable, BlockWritable, LongWritable, BlockWritable> mapDriver;
    ReduceDriver<LongWritable, BlockWritable, LongWritable, BlockWritable> reduceDriver;

    @Before
    public void setUp() {
        ConCmptBlock.MapStage1 mapper = new ConCmptBlock.MapStage1();
        mapDriver = MapDriver.newMapDriver(mapper);

        ConCmptBlock.RedStage1 reducer = new ConCmptBlock.RedStage1();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void map() throws IOException {
        BlockIndexWritable b1 = new BlockIndexWritable();
        BlockIndexWritable b2 = new BlockIndexWritable();

        BlockWritable d1 = new BlockWritable();
        BlockWritable d2 = new BlockWritable();

        b1.setVectorIndex(1);
        d1.setTypeVector(5);
        d1.setVectorElem(1, 11);
        d1.setVectorElem(2, 12);
        d1.setVectorElem(3, 13);
        d1.setVectorElem(4, 14);

        b2.setMatrixIndex(0, 1);
        d2.setTypeMatrix();
        d2.addMatrixElem(1, 2);
        d2.addMatrixElem(1, 3);
        d2.addMatrixElem(4, 5);

        mapDriver.addInput(b1, d1);
        mapDriver.addInput(b2, d2);

        BlockWritable v1 = new BlockWritable();
        v1.setTypeVector(5);
        v1.setVectorElem(0, -1);
        v1.setVectorElem(1, 11);
        v1.setVectorElem(2, 12);
        v1.setVectorElem(3, 13);
        v1.setVectorElem(4, 14);

        BlockWritable v2 = new BlockWritable();
        v2.setTypeMatrix();
        v2.setBlockRow(0);
        v2.addMatrixElem(1, 2);
        v2.addMatrixElem(1, 3);
        v2.addMatrixElem(4, 5);

        mapDriver.addOutput(new LongWritable(1), v1);
        mapDriver.addOutput(new LongWritable(1), v2);

        mapDriver.runTest();
    }

    //
    //     |0 1 0|      |0|        |0|
    // M = |1 0 1|  V = |1|  res = |0|
    //     |0 1 0|      |2|        |1|
    @Test
    public void reduce() throws IOException {
        reduceDriver.getConfiguration().setInt("block_width", 3);
        reduceDriver.getConfiguration().setInt("recursive_diagmult", 0);

        int block_col = 1;

        BlockWritable e1 = new BlockWritable();
        e1.setTypeVector(3);
        e1.setVectorElem(0, 0);
        e1.setVectorElem(1, 1);
        e1.setVectorElem(2, 2);

        BlockWritable e2 = new BlockWritable();
        e2.setTypeMatrix();
        e2.addMatrixElem(0, 1);
        e2.addMatrixElem(1, 0);
        e2.addMatrixElem(1, 2);
        e2.addMatrixElem(2, 1);

        e2.setBlockRow(block_col);

        reduceDriver.addInput(new LongWritable(block_col), Arrays.asList(e1, e2));

        BlockWritable v1 = new BlockWritable();
        v1.setVector(BlockWritable.TYPE.MSI, new TLongArrayList(
                new long[] {0, 1, 2}));

        BlockWritable v2 = new BlockWritable();
        v2.setVector(BlockWritable.TYPE.MOI, new TLongArrayList(
                new long[]{0, 0, 1}));

        reduceDriver.addOutput(new LongWritable(block_col), v1); // initial vector
        reduceDriver.addOutput(new LongWritable(block_col), v2); // after multiplication
        reduceDriver.runTest();
    }
}

