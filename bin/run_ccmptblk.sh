JAR=../target/Pegasus-1.3-SNAPSHOT-fatjar.jar

WORK_DIR=pegasus_work

N_NODES=$1
N_REDUCERS=$2
INPUT_EDGES=$3
BLOCK_SIZE=$4

hadoop dfs -rm -r $WORK_DIR

hadoop jar $JAR pegasus.InitialVectorGenerator $WORK_DIR/initial_vector $N_NODES $N_REDUCERS
hadoop jar $JAR pegasus.BlocksBuilder $WORK_DIR/initial_vector $WORK_DIR/vector_blocks $BLOCK_SIZE $N_REDUCERS vector
hadoop jar $JAR pegasus.BlocksBuilder $INPUT_EDGES $WORK_DIR/edge_blocks $BLOCK_SIZE $N_REDUCERS matrix
hadoop jar $JAR pegasus.Runner $WORK_DIR/edge_blocks $WORK_DIR/vector_blocks $WORK_DIR $N_REDUCERS $BLOCK_SIZE
