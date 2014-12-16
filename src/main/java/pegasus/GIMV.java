/***********************************************************************
 PEGASUS: Peta-Scale Graph Mining System
 Copyright (C) 2010 U Kang, Duen Horng Chau, and Christos Faloutsos
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

import gnu.trove.list.array.TLongArrayList;
import gnu.trove.list.array.TShortArrayList;

public class GIMV {
    private final static long NO_VALUE = -1L;

    public static TLongArrayList minBlockVector(TShortArrayList matrixIndexes,
                                                TLongArrayList vectorValues)
    {
        TLongArrayList output = new TLongArrayList(vectorValues.size());
        output.fill(0, vectorValues.size(), NO_VALUE);
        int max = matrixIndexes.size() / 2;
        for (int i = 0; i < max; i++) {
            short matrixElementRow = matrixIndexes.getQuick(2 * i);
            if (vectorValues.getQuick(matrixElementRow) == NO_VALUE) {
                continue;
            }
            short matrixElementColumn = matrixIndexes.getQuick(2 * i + 1);
            long val = vectorValues.getQuick(matrixElementColumn);
            long currentVal = output.getQuick(matrixElementRow);
            if (val != NO_VALUE && (currentVal == NO_VALUE || val < currentVal)) {
                output.setQuick(matrixElementRow, val);
            }
        }
        return output;
    }

    public static TLongArrayList minBlockVector(BlockWritable block, BlockWritable vect) {
        return minBlockVector(block.getMatrixElemIndexes(), vect.getVectorElemValues());
    }
};

