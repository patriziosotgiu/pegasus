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

import gnu.trove.list.array.TLongArrayList;
import gnu.trove.list.array.TShortArrayList;

public class GIMV {
    public static TLongArrayList minBlockVector(TShortArrayList matrixIndexes,
                                                TLongArrayList vectorValues)
    {
        TLongArrayList output = new TLongArrayList(vectorValues);
        int i = 0;
        while (i < matrixIndexes.size() / 2) {
            short row = matrixIndexes.get(2 * i);
            short col = matrixIndexes.get(2 * i + 1);
            long val = vectorValues.get(col);
            if (val < output.get(row)) {
                output.set(row, val);
            }
            i++;
        }
        return output;
    }
};

