/**
 * PIQASUS: Connected-component analysis for Big Graph
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
 */

package com.placeiq.piqasus;

import com.google.common.base.Objects;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BlockIndexWritable implements WritableComparable<BlockIndexWritable> {

    private boolean isVector = true;

    long i;
    long j;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        if (isVector) {
            WritableUtils.writeVLong(dataOutput, - (i + 1));
        }
        else {
            WritableUtils.writeVLong(dataOutput, i + 1);
            WritableUtils.writeVLong(dataOutput, j);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        long v = WritableUtils.readVLong(dataInput);
        if (v < 0) {
            isVector = true;
            i = -v - 1;
        }
        else {
            isVector = false;
            i = v - 1;
            j = WritableUtils.readVLong(dataInput);
        }
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("isVector", isVector)
                .add("i", i)
                .add("j", j)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BlockIndexWritable that = (BlockIndexWritable) o;

        if (i != that.i) return false;
        if (isVector != that.isVector) return false;
        if (j != that.j) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (isVector ? 1 : 0);
        result = 31 * result + (int) (i ^ (i >>> 32));
        result = 31 * result + (int) (j ^ (j >>> 32));
        return result;
    }

    public void setVectorIndex(long i) {
        isVector = true;
        this.i = i;
    }

    public void setMatrixIndex(long i, long j) {
        isVector = false;
        this.i = i;
        this.j = j;
    }

    public long getI() {
        return i;
    }

    public long getJ() {
        return j;
    }

    @Override
    public int compareTo(BlockIndexWritable o) {
        if (this.isVector && !o.isVector) {
            return -1;
        }
        else if (!this.isVector && o.isVector) {
            return 1;
        }
        int cmp = Long.compare(this.i, o.i);
        if (cmp != 0) {
            return cmp;
        }
        return Long.compare(this.j, o.j);
    }

    public static BlockIndexWritable newVectorBlock(int i) {
        BlockIndexWritable res = new BlockIndexWritable();
        res.setVectorIndex(i);
        return res;
    }

    public static BlockIndexWritable newMatrixBlock(int i, int j) {
        BlockIndexWritable res = new BlockIndexWritable();
        res.setMatrixIndex(i, j);
        return res;
    }
}