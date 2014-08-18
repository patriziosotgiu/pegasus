/***********************************************************************
 PEGASUS: Peta-Scale Graph Mining System
 Authors: U Kang, Duen Horng Chau, and Christos Faloutsos

 This software is licensed under Apache License, Version 2.0 (the  "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 -------------------------------------------------------------------------
 File: GIMV.java
 - A main class for Generalized Iterative Matrix-Vector multiplication.
 Version: 2.0
 ***********************************************************************/

package pegasus;

import com.google.common.base.Objects;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

class VectorElem {
    public short row;
    public long val;

    public VectorElem(short in_row, long in_val) {
        row = in_row;
        val = in_val;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("row", row)
                .add("val", val)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VectorElem that = (VectorElem) o;

        if (row != that.row) return false;
        if (val != that.val) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) row;
        result = 31 * result + (int) (val ^ (val >>> 32));
        return result;
    }
};

class BlockElem {
    public short row;
    public short col;
    public long val;

    public BlockElem(short in_row, short in_col, long in_val) {
        row = in_row;
        col = in_col;
        val = in_val;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("row", row)
                .add("col", col)
                .add("val", val)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BlockElem blockElem = (BlockElem) o;

        if (col != blockElem.col) return false;
        if (row != blockElem.row) return false;
        if (val != blockElem.val) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) row;
        result = 31 * result + (int) col;
        result = 31 * result + (int) (val ^ (val >>> 32));
        return result;
    }
};

public class GIMV {

    public static ArrayList<VectorElem> minBlockVector(ArrayList<BlockElem> block, ArrayList<VectorElem> vector, int block_width, int isFastMethod) {
        long[] out_vals = new long[block_width];    // buffer to save output
        short i;

        for (i = 0; i < block_width; i++)
            out_vals[i] = -1;

        Iterator<VectorElem> vector_iter;
        Iterator<BlockElem> block_iter;
        Map<Short, Long> vector_map = new HashMap<Short, Long>();

        // initialize out_vals
        if (isFastMethod == 1) {
            vector_iter = vector.iterator();
            while (vector_iter.hasNext()) {
                VectorElem v_elem = vector_iter.next();
                out_vals[v_elem.row] = v_elem.val;
            }
        }

        vector_iter = vector.iterator();
        block_iter = block.iterator();
        BlockElem saved_b_elem = null;

        while (vector_iter.hasNext()) {
            VectorElem v_elem = vector_iter.next();
            vector_map.put(v_elem.row, v_elem.val);
        }


        BlockElem b_elem;
        while (block_iter.hasNext() || saved_b_elem != null) {
            b_elem = block_iter.next();

            Long vector_val = vector_map.get(b_elem.col);
            if (vector_val != null) {
                long vector_val_long = vector_val.longValue();
                if (out_vals[b_elem.row] == -1)
                    out_vals[b_elem.row] = vector_val_long;
                else if (out_vals[b_elem.row] > vector_val_long)
                    out_vals[b_elem.row] = vector_val_long;
            }
        }

        ArrayList<VectorElem> result_vector = null;
        for (i = 0; i < block_width; i++) {
            if (out_vals[i] != -1) {
                if (result_vector == null)
                    result_vector = new ArrayList<VectorElem>();
                result_vector.add(new VectorElem(i, out_vals[i]));
            }
        }

        return result_vector;
    }

    // compare two vectors.
    // return value : 0 (same)
    //                1 (different)
    public static int compareVectors(ArrayList<VectorElem> v1, ArrayList<VectorElem> v2) {
        if (v1.size() != v2.size())
            return 1;
        Iterator<VectorElem> v1_iter = v1.iterator();
        Iterator<VectorElem> v2_iter = v2.iterator();
        while (v1_iter.hasNext()) {
            VectorElem elem1 = v1_iter.next();
            VectorElem elem2 = v2_iter.next();
            if (elem1.row != elem2.row || elem1.val != elem2.val)
                return 1;
        }
        return 0;
    }

    public static ArrayList<VectorElem> makeLongVectors(long[] int_vals, int block_width) {
        int i;
        ArrayList<VectorElem> result_vector = new ArrayList<VectorElem>();

        for (i = 0; i < block_width; i++) {
            if (int_vals[i] != -1) {
                result_vector.add(new VectorElem((short) i, int_vals[i]));
            }
        }
        return result_vector;
    }
};

