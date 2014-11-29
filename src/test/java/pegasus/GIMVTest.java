package pegasus;

import gnu.trove.list.array.TLongArrayList;
import gnu.trove.list.array.TShortArrayList;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class GIMVTest {

    //
    //     |0 1 0|      |0|        |1|
    // M = |1 0 1|  V = |1|  res = |0|
    //     |0 1 0|      |2|        |1|
    @Test
    public void simple() throws IOException {
        TShortArrayList matrixIndexes = new TShortArrayList(new short[] {0, 1, 1, 0, 1, 2, 2, 1});
        TLongArrayList vectorValues = new TLongArrayList(new long[] {0, 1, 2});
        TLongArrayList res = new TLongArrayList(new long[] {1, 0, 1});
        assertEquals(res, GIMV.minBlockVector(matrixIndexes, vectorValues));
    }

    @Test
    public void simple2() throws IOException {
        TShortArrayList matrixIndexes = new TShortArrayList(new short[] {0, 1, 1, 0, 1, 2, 2, 1, 3, 3});
        TLongArrayList vectorValues = new TLongArrayList(new long[] {0, 1, 2, 3});
        TLongArrayList res = new TLongArrayList(new long[] {1, 0, 1, 3});
        assertEquals(res, GIMV.minBlockVector(matrixIndexes, vectorValues));
    }

    //
    //     |0 1|      |0|        |1|
    // M = |0 1|  V = |1|  res = |1|
    @Test
    public void simple3() throws IOException {
        TShortArrayList matrixIndexes = new TShortArrayList(new short[] {0, 1, 1, 1});
        TLongArrayList vectorValues = new TLongArrayList(new long[] {0, 1});
        TLongArrayList res = new TLongArrayList(new long[] {1, 1});
        assertEquals(res, GIMV.minBlockVector(matrixIndexes, vectorValues));
    }

    //
    //     |0 0 0|      |3 |        |3|
    // M = |0 0 0|  V = |-1|  res = |-1|
    //     |1 0 0|      |-1|        |-1|
    @Test
    public void simple4() throws IOException {
        TShortArrayList matrixIndexes = new TShortArrayList(new short[] {2, 0});
        TLongArrayList vectorValues = new TLongArrayList(new long[] {3, -1, -1});
        TLongArrayList res = new TLongArrayList(new long[] {3, -1, -1});
        assertEquals(res, GIMV.minBlockVector(matrixIndexes, vectorValues));
    }
}

