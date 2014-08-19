package pegasus;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class ConCmptBlockTest {
    MapDriver<LongWritable, Text, LongWritable, ElemArrayWritable> mapDriver;
    ReduceDriver<LongWritable, ElemArrayWritable, LongWritable, VectorElemWritable> reduceDriver;


    @Before
    public void setUp() {
        ConCmptBlock.MapStage1 mapper = new ConCmptBlock.MapStage1();
        mapDriver = MapDriver.newMapDriver(mapper);

        ConCmptBlock.RedStage1 reducer = new ConCmptBlock.RedStage1();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);

    }

    @Test
    public void map() throws IOException {
        mapDriver.addInput(new LongWritable(0), new Text("1\tmsc1 11 2 12 3 13 4 14"));
        mapDriver.addInput(new LongWritable(1), new Text("1\t2\t1 2 1 3 4 5"));

        ElemArrayWritable v1 = new ElemArrayWritable();
        v1.addVector((short) 1, 11);
        v1.addVector((short) 2, 12);
        v1.addVector((short) 3, 13);
        v1.addVector((short) 4, 14);

        ElemArrayWritable v2 = new ElemArrayWritable();
        v2.addBlock((short)2, (short)1, 1);
        v2.addBlock((short)3, (short)1, 1);
        v2.addBlock((short)5, (short)4, 1);

        mapDriver.addOutput(new LongWritable(1), v1);
        mapDriver.addOutput(new LongWritable(2), v2);
        mapDriver.runTest();
    }

    //
    //     |0 1 0|      |1|        |2|
    // M = |0 0 1|  V = |2|  res = |3|
    //     |0 0 0|      |3|        |0|
    @Test
    public void reduce() throws IOException {
        reduceDriver.getConfiguration().setInt("block_width", 3);
        reduceDriver.getConfiguration().setInt("recursive_diagmult", 1);

        ElemArrayWritable e1 = new ElemArrayWritable();
        e1.addVector((short) 0, 1);
        e1.addVector((short) 1, 2);
        e1.addVector((short) 2, 3);

        ElemArrayWritable e2 = new ElemArrayWritable();
        e2.addBlock((short) 0, (short) 1, 1);
        e2.addBlock((short) 1, (short) 2, 1);
        e2.setBlockCol(1);

        reduceDriver.addInput(new LongWritable(1), Arrays.asList(e1, e2));

        VectorElemWritable v1 = new VectorElemWritable();
        v1.set(VectorElemWritable.TYPE.MSI, new ArrayList<VectorElem>() {{
            add(new VectorElem((short)0, 1));
            add(new VectorElem((short)1, 2));
            add(new VectorElem((short)2, 3));
        }});

        VectorElemWritable v2 = new VectorElemWritable();
        v2.set(VectorElemWritable.TYPE.MOI, new ArrayList<VectorElem>() {{
            add(new VectorElem((short)0, 2));
            add(new VectorElem((short)1, 3));
        }});


        reduceDriver.addOutput(new LongWritable(1), v1); // initial vector
        reduceDriver.addOutput(new LongWritable(-1), v2);    // after multiplication
        reduceDriver.runTest();
    }
}

