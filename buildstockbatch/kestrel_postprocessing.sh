#!/bin/bash
#SBATCH --tmp=1000000

echo "begin kestrel_postprocessing.sh"

echo "Job ID: $SLURM_JOB_ID"
echo "Hostname: $HOSTNAME"

df -i
df -h

module load python apptainer
source "$MY_PYTHON_ENV/bin/activate"
export LOCAL_SCRATCH=$TMPDIR # $TMPDIR is set to /tmp/scratch/$JOBID
source /kfs2/shared-projects/buildstock/aws_credentials.sh

export POSTPROCESS=1

echo "UPLOADONLY: ${UPLOADONLY}"
echo "MEMORY: ${MEMORY}"
echo "NPROCS: ${NPROCS}"

SCHEDULER_FILE=$OUT_DIR/dask_scheduler.json

echo "head node"
echo $SLURM_JOB_NODELIST_PACK_GROUP_0=$HEAD_JOB_ID # inherited from changes to hpc.py
echo "workers"

# inherited from changes to hpc.py
if [ $WORKER_JOB=1 ]; then 
	export WORKER_JOB_ID=$SLURM_JOB_ID
fi

export SLURM_JOB_NODELIST_PACK_GROUP_1=$WORKER_JOB_ID # inherited from changes to hpc.py
echo $SLURM_JOB_NODELIST_PACK_GROUP_1

pdsh -w $SLURM_JOB_NODELIST_PACK_GROUP_1 "free -h"
pdsh -w $SLURM_JOB_NODELIST_PACK_GROUP_1 "df -i; df -h"

$MY_PYTHON_ENV/bin/dask scheduler --scheduler-file $SCHEDULER_FILE &> $OUT_DIR/dask_scheduler.out &
pdsh -w $SLURM_JOB_NODELIST_PACK_GROUP_1 "source /kfs2/shared-projects/buildstock/aws_credentials.sh; $MY_PYTHON_ENV/bin/dask worker --scheduler-file $SCHEDULER_FILE --local-directory $LOCAL_SCRATCH/dask --nworkers ${NPROCS} --nthreads 1 --memory-limit ${MEMORY}MB" &> $OUT_DIR/dask_workers.out &

time python -u -m buildstockbatch.hpc kestrel "$PROJECTFILE"
