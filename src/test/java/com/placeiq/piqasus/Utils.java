/**
 * PIQASUS: Connected-component analysis for Big Graph
 *
 * __________.___________      _____    _____________ ___  _________
 * \______   \   \_____  \    /  _  \  /   _____/    |   \/   _____/
 *  |     ___/   |/  / \  \  /  /_\  \ \_____  \|    |   /\_____  \
 *  |    |   |   /   \_/.  \/    |    \/        \    |  / /        \
 *  |____|   |___\_____\ \_/\____|__  /_______  /______/ /_______  /
 *                      \__>        \/        \/                 \/
 *
 * Copyright (c) 2014 PlaceIQ, Inc
 *
 * This software is licensed under Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * ----------------------------------------------------------------------------
 * Author: Jerome Serrano <jerome.serrano@placeiq.com>
 * Date: 2015-01-09
 * ---------------------------------------------------------------------------*/

package com.placeiq.piqasus;

import gnu.trove.list.array.TLongArrayList;

public class Utils {

    public static BlockIndexWritable blockIndex(int i, int j) {
        return BlockIndexWritable.newMatrixBlock(i, j);
    }

    public static BlockIndexWritable blockIndex(int i) {
        return BlockIndexWritable.newVectorBlock(i);
    }


    public static BlockWritable blockVector(BlockWritable.TYPE type, long ... data) {
        BlockWritable res = new BlockWritable();
        res.setVector(type, new TLongArrayList(data));
        return res;
    }

    public static BlockWritable blockVector(long ... data) {
        BlockWritable res = new BlockWritable(data.length, BlockWritable.TYPE.VECTOR_INITIAL);
        for (int i = 0; i < data.length; i++) {
            res.setVectorElem(i, data[i]);
        }
        return res;
    }

    public static BlockWritable blockMatrix(long blockRow, int...data) {
        BlockWritable res = new BlockWritable(data.length / 2, BlockWritable.TYPE.MATRIX);
        res.setBlockRow(blockRow);
        for (int i = 0; i < data.length / 2; i++) {
            res.addMatrixElem(data[2 * i], data[2 * i + 1]);
        }
        return res;
    }
}
