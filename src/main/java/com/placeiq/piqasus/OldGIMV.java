package com.placeiq.piqasus;

import gnu.trove.list.array.TLongArrayList;
import gnu.trove.list.array.TShortArrayList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

class VectorElem<T> {
    public short row;
    public T val;

    public VectorElem(short in_row, T in_val) {
        row = in_row;
        val = in_val;
    }

};

class BlockElem<T> {
    public short row;
    public short col;
    public T val;

    public BlockElem(short in_row, short in_col, T in_val) {
        row = in_row;
        col = in_col;
        val = in_val;
    }
};


public class OldGIMV {

    public static TLongArrayList minBlockVector(TShortArrayList matrixIndexes,
                                                TLongArrayList vectorValues)
    {
        TLongArrayList output = new TLongArrayList(vectorValues.size());
        output.fill(0, vectorValues.size(), -1);


        ArrayList<BlockElem<Long>> block = new ArrayList<BlockElem<Long>>();
        ArrayList<VectorElem<Long>> vector = new ArrayList<VectorElem<Long>>();

        for (int i = 0; i < matrixIndexes.size() / 2; i++) {
            BlockElem b = new BlockElem(matrixIndexes.get(i * 2), matrixIndexes.get(i * 2 + 1), 0);
            block.add(b);
        }
        for (int i = 0; i < vectorValues.size() ; i++) {
            VectorElem v = new VectorElem((short)i, vectorValues.get(i));
            vector.add(v);
        }


        ArrayList<VectorElem<Long>> res = minBlockVector(block, vector, vectorValues.size(), 0);

        for (int i = 0; i < res.size(); i++) {
            output.set(res.get(i).row, res.get(i).val);
        }

        return output;
    }


    public static ArrayList<VectorElem<Long>> minBlockVector(ArrayList<BlockElem<Long>> block, ArrayList<VectorElem<Long>> vector, int block_width, int isFastMethod) {
        long[] out_vals = new long[block_width];    // buffer to save output
        short i;

        for (i = 0; i < block_width; i++)
            out_vals[i] = -1;

        Iterator<VectorElem<Long>> vector_iter;
        Iterator<BlockElem<Long>> block_iter;
        Map<Short, Long> vector_map = new HashMap<Short, Long>();

        // initialize out_vals
        if (isFastMethod == 1) {
            vector_iter = vector.iterator();
            while (vector_iter.hasNext()) {
                VectorElem<Long> v_elem = vector_iter.next();
                out_vals[v_elem.row] = v_elem.val;
            }
        }

        vector_iter = vector.iterator();
        block_iter = block.iterator();
        BlockElem<Long> saved_b_elem = null;

        while (vector_iter.hasNext()) {
            VectorElem<Long> v_elem = vector_iter.next();
            vector_map.put(v_elem.row, v_elem.val);
        }


        BlockElem<Long> b_elem;
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

        ArrayList<VectorElem<Long>> result_vector = null;
        for (i = 0; i < block_width; i++) {
            if (out_vals[i] != -1) {
                if (result_vector == null)
                    result_vector = new ArrayList<VectorElem<Long>>();
                result_vector.add(new VectorElem<Long>(i, out_vals[i]));
            }
        }

        return result_vector;
    }
};

