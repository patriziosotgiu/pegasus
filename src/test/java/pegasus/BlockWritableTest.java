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

