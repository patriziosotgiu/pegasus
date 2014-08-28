package pegasus;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;
import pegasus.matvec.MatvecPrep;

import java.io.IOException;

public class MatvecPrepTest {
    MapDriver<LongWritable, Text, BlockIndexWritable, BlockWritable> mapDriver;
    ReduceDriver<BlockIndexWritable, BlockWritable, BlockIndexWritable, BlockWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, BlockIndexWritable, BlockWritable, BlockIndexWritable, BlockWritable> mrDriver;


    @Before
    public void setUp() {
        MatvecPrep.MapStage1 mapper = new MatvecPrep.MapStage1();
        mapDriver = MapDriver.newMapDriver(mapper);
        MatvecPrep.RedStage1 reducer = new MatvecPrep.RedStage1();
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
    public void simple() throws IOException {
        mrDriver.getConfiguration().set("block_size", "3");
        mrDriver.getConfiguration().set("matrix_row", "6");
        mrDriver.getConfiguration().set("makesym", "1");

        mrDriver.addInput(new LongWritable(0), new Text("1\t2"));
        mrDriver.addInput(new LongWritable(0), new Text("1\t3"));
        mrDriver.addInput(new LongWritable(0), new Text("3\t4"));

        BlockIndexWritable b1 = new BlockIndexWritable();
        BlockIndexWritable b2 = new BlockIndexWritable();
        BlockIndexWritable b3 = new BlockIndexWritable();
        BlockIndexWritable b4 = new BlockIndexWritable();

        BlockWritable d1 = new BlockWritable();
        BlockWritable d2 = new BlockWritable();
        BlockWritable d3 = new BlockWritable();
        BlockWritable d4 = new BlockWritable();

        d1.setTypeMatrix();
        d3.setTypeMatrix();
        d2.setTypeMatrix();
        d4.setTypeMatrix();

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
}

