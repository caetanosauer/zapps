#!/bin/bash

MAX_THREADS=20

SF=$1
DURATION=$2

DEVTYPE="ssd"
WORKSPACE=20000
BUFSIZE=90000
LOGSIZE=300000

[ ! -z "$SF" ] || exit 1
[ ! -z "$DURATION" ] || DURATION=300

THREADS=$(( MAX_THREADS > SF ? SF : MAX_THREADS ))

set -e

BASEDIR=/mnt/snap/
LOADDIR=$BASEDIR/load/tpcc-$SF
BENCHDIR=$BASEDIR/bench/"$DURATION"s/tpcc-$SF
ARCHDIR=/mnt/archive-$DEVTYPE/archive
LOGDIR=/mnt/log-$DEVTYPE/log 
mkdir -p $BASEDIR

[ -d $BASEDIR ] || exit 1

mkdir -p $LOADDIR
rm -rf $LOADDIR/*

mkdir -p $BENCHDIR
rm -rf $BENCHDIR/*

echo -n "Loading DB ... "

# DEBUG_FLAGS="restore.cpp logarchiver.cpp" gdb --args \
zapps kits -b tpcc --load \
    -d paths/db --backup paths/backup -l $LOGDIR -a $ARCHDIR \
    --bufsize $BUFSIZE --logsize $LOGSIZE --quota 300000 --archWorkspace $WORKSPACE \
    -q $SF -t $THREADS \
    --eager --sharpBackup true --truncateLog true \
    --sm_archiver_bucket_size 128 \
    1> out1.txt 2> out2.txt

echo "OK"

echo -n "Copying data ... "
# backup is not required -- it will be copied to bench dir below
mkdir -p $LOADDIR/archive
rsync -aq --copy-links --delete $ARCHDIR/ $LOADDIR/archive/
mkdir -p $LOADDIR/log
rsync -aq --copy-links --delete $LOGDIR/ $LOADDIR/log/
rsync -aq --copy-links --delete paths/db $LOADDIR/db
cp out?.txt $LOADDIR/
echo "OK"

echo -n "Running benchmark ... "

# gdb --args \
zapps kits -b tpcc \
    -l $LOGDIR -a $ARCHDIR \
    --bufsize $BUFSIZE --logsize $LOGSIZE --archWorkspace $((WORKSPACE / 10)) \
    -q $SF -t $THREADS \
    --eager --truncateLog --duration $DURATION \
    --sm_archiver_bucket_size 128 \
    1> out1.txt 2> out2.txt

echo "OK"

echo -n "Copying data ... "
rsync -aq --copy-links --delete paths/ $BENCHDIR/
rsync -aq --copy-links --delete $ARCHDIR/ $BENCHDIR/archive/
rsync -aq --copy-links --delete $LOGDIR/ $BENCHDIR/log/
cp out?.txt $BENCHDIR/
echo "OK"
