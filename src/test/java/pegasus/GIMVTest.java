package pegasus;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class GIMVTest {

    //
    //     |0 1 0|      |1|        |2|
    // M = |0 0 1|  V = |2|  res = |3|
    //     |0 0 0|      |3|        |0|
    @Test
    public void simple() throws IOException {
        ArrayList<VectorElem> vectors = new ArrayList<VectorElem>();
        ArrayList<BlockElem> blocks = new ArrayList<BlockElem>();

        vectors.add(new VectorElem((short)0, 1));
        vectors.add(new VectorElem((short)1, 2));
        vectors.add(new VectorElem((short)2, 3));

        blocks.add(new BlockElem((short)0, (short)1, 1));
        blocks.add(new BlockElem((short)1, (short)2, 1));

        ArrayList<VectorElem> res = new ArrayList<VectorElem>();
        res.add(new VectorElem((short)0, 2));
        res.add(new VectorElem((short)1, 3));

        assertEquals(res, GIMV.minBlockVector(blocks, vectors, 3, 0));
    }
}

