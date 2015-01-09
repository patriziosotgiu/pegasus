# PIQASUS: Connected-component analysis for Big Graph
#
# __________.___________      _____    _____________ ___  _________
# \______   \   \_____  \    /  _  \  /   _____/    |   \/   _____/
#  |     ___/   |/  / \  \  /  /_\  \ \_____  \|    |   /\_____  \
#  |    |   |   /   \_/.  \/    |    \/        \    |  / /        \
#  |____|   |___\_____\ \_/\____|__  /_______  /______/ /_______  /
#                      \__>        \/        \/                 \/
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
# ----------------------------------------------------------------------------
# Author: Jerome Serrano <jerome.serrano@placeiq.com>
# Date: 2015-01-09
# ----------------------------------------------------------------------------



#!/bin/sh

JAR=../target/Pegasus-1.3-SNAPSHOT-fatjar.jar

WORK_DIR=pegasus_work

N_NODES=$1
N_REDUCERS_RUNNER=$2
N_REDUCERS_BLOCKS=$2
INPUT_EDGES=$3
BLOCK_SIZE=$4
MAX_CONVERGENCE=$5
MAX_ITERATIONS=$6

HADOOP_OPTS_RUNNER='-D mapred.map.child.java.opts=-Xmx2048M -D io.sort.mb=1024 -D io.sort.record.percent=0.7 -D mapred.job.reuse.jvm.num.tasks=-1'
HADOOP_OPTS_BLOCK="-D io.sort.mb=716 -D io.sort.record.percent=0.7 -D mapred.job.reuse.jvm.num.tasks=-1 -D mapred.job.reduce.input.buffer.percent=0.7"

HADOOP="hadoop jar $JAR"

hadoop dfs -rm -r $WORK_DIR

$HADOOP com.placeiq.piqasus.InitialVectorGenerator $WORK_DIR/initial_vector $N_NODES $N_REDUCERS_BLOCKS
$HADOOP com.placeiq.piqasus.BlocksBuilder $HADOOP_OPTS_BLOCK $WORK_DIR/initial_vector $WORK_DIR/vector_blocks $BLOCK_SIZE $N_REDUCERS_BLOCKS vector
$HADOOP com.placeiq.piqasus.BlocksBuilder $HADOOP_OPTS_BLOCK $INPUT_EDGES $WORK_DIR/edge_blocks $BLOCK_SIZE $N_REDUCERS_BLOCKS matrix
$HADOOP com.placeiq.piqasus.Runner $HADOOP_OPTS_RUNNER $WORK_DIR/edge_blocks $WORK_DIR/vector_blocks $WORK_DIR $N_REDUCERS_BLOCKS $BLOCK_SIZE $MAX_CONVERGENCE $MAX_ITERATIONS