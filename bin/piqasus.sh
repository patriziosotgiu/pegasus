# PIQASUS: Connected-component analysis for Big Graph
#
# Copyright (c) 2014 PlaceIQ, Inc
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/bin/sh

JAR=../target/Pegasus-1.3-SNAPSHOT-fatjar.jar

WORK_DIR=pegasus_work

N_NODES=$1
N_REDUCERS=$2
INPUT_EDGES=$3
BLOCK_SIZE=$4
MAX_CONVERGENCE=$5
MAX_ITERATIONS=$6

hadoop dfs -rm -r $WORK_DIR

hadoop jar $JAR com.placeiq.piqasus.InitialVectorGenerator $WORK_DIR/initial_vector $N_NODES $N_REDUCERS
hadoop jar $JAR com.placeiq.piqasus.BlocksBuilder $WORK_DIR/initial_vector $WORK_DIR/vector_blocks $BLOCK_SIZE $N_REDUCERS vector
hadoop jar $JAR com.placeiq.piqasus.BlocksBuilder $INPUT_EDGES $WORK_DIR/edge_blocks $BLOCK_SIZE $N_REDUCERS matrix
hadoop jar $JAR com.placeiq.piqasus.Runner $WORK_DIR/edge_blocks $WORK_DIR/vector_blocks $WORK_DIR $N_REDUCERS $BLOCK_SIZE $MAX_CONVERGENCE $MAX_ITERATIONS
