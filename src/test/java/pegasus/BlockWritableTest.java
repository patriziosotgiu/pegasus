package pegasus;

import org.junit.Test;

import java.io.*;

import static org.junit.Assert.assertEquals;

public class BlockWritableTest {

    @Test
    public void serializeVector() throws IOException {
        BlockWritable b1 = new BlockWritable();
        b1.setTypeVector(6);
        b1.setVectorElem(1, 10L);
        b1.setVectorElem(3, 30L);
        b1.setVectorElem(5, 50L);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream w = new DataOutputStream(baos);
        b1.write(w);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream r = new DataInputStream(bais);
        BlockWritable b2 = new BlockWritable();
        b2.readFields(r);

        assertEquals(b1, b2);
    }

    @Test
    public void serializeMatrix() throws IOException {
        BlockWritable b1 = new BlockWritable();
        b1.addMatrixElem(1, 10);
        b1.addMatrixElem(3, 30);
        b1.addMatrixElem(5, 50);
        b1.setBlockRow(10);
        b1.setTypeMatrix();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream w = new DataOutputStream(baos);
        b1.write(w);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream r = new DataInputStream(bais);
        BlockWritable b2 = new BlockWritable();
        b2.readFields(r);

        assertEquals(b1, b2);
    }
}

