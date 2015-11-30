#!/bin/bash

function die() { echo >&2 "$@"; exit 1; }

# Arguments
# $1 list of segment sizes for which to run experiment
#    e.g., "128 1024 8192"
SIZES=$1
# $2 list of log archive suffixes (e.g., "1 2 3")
ARCHIVES=$2
# $3 type of restore (multseg | normal)
RESTORE_TYPE=$3
# $4 type of device (hdd | ssd)
DEVTYPE=$4

[ -n "$SIZES" ] || die "Invalid sizes parameter"
[ -n "$ARCHIVES" ] || die "Invalid archive suffix parameter"
[[ "$RESTORE_TYPE" == "multseg" ]] || [[ "$RESTORE_TYPE" == "normal" ]] \
    || die "Invalid restore type parameter"
[[ "$DEVTYPE" == "hdd" ]] || [[ "$DEVTYPE" == "ssd" ]] \
    || die "Invalid device type parameter"

set -e

SEQ_SEGSIZE=16384
MAX_UNIT_FACTOR=2

SRCDIR=/mnt/snap/bench/300s/tpcc-100
[ -d $SRCDIR ] || (echo "Folder not found: $SRCDIR"; exit 1)

mkdir -p paths
rm -f paths/*

for a in $ARCHIVES; do
    EXPNAME="$RESTORE_TYPE-$DEVTYPE-arch$a"
    for i in $1; do
        echo "=== RUNNING $EXPNAME FOR $i ==="

        EXPDIR=~/experiments/offline/$EXPNAME/restore-$i
        mkdir -p $EXPDIR

        # COPY DB
        [ -f $SRCDIR/db ] || (echo "DB file not found in $SRCDIR"; exit 1)

        echo -n "Copying DB file ... "
        DBDIR=/mnt/db-$DEVTYPE
        rsync -a $SRCDIR/db $DBDIR/old/db
        if [ -d $DBDIR/new ]; then
            btrfs subvolume delete $DBDIR/new
        fi
        btrfs subvolume snapshot $DBDIR/old $DBDIR/new
        ln -fs $DBDIR/new/db paths/db
        echo "OK"

        # COPY LOG AND ARCHIVE
        [ -d $SRCDIR/log ] || (echo "Log dir not found in $SRCDIR"; exit 1)
        [ -d $SRCDIR/archive_$a ] || (echo "Archive not found in $SRCDIR/archive_$a"; exit 1)

        echo -n "Copying log archive (number $a)  and recovery log ... "
        rsync -a --delete $SRCDIR/archive_$a/ /mnt/archive-$DEVTYPE/archive/
        rsync -a --delete $SRCDIR/log/ /mnt/log-$DEVTYPE/log/
        echo "OK"

        # COPY BACKUP
        [ -f $SRCDIR/backup ] || (echo "Backup file not found in $SRCDIR"; exit 1)

        echo -n "Copying backup file ... "
        rsync -a $SRCDIR/backup /mnt/backup-$DEVTYPE/backup
        ln -fs /mnt/backup-$DEVTYPE/backup paths/backup
        echo "OK"

        iostat -dmtxy 1 1> iostat.txt 2> /dev/null &
        IOSTAT_PID=$!

        mpstat 1 1> mpstat.txt 2> /dev/null &
        MPSTAT_PID=$!

        if [[ "$i" == "seq" ]]; then
            ARG="--instant false --segmentSize $SEQ_SEGSIZE"
        elif [[ "$RESTORE_TYPE" == "multseg" ]]; then
            # convert $i from kbytes to bytes
            min=$((i * 1024))
            max=$((min * MAX_UNIT_FACTOR))
            segsize=$((max / 8192))
            maxseg=1000
            ARG="--segmentSize $segsize --sm_restore_min_read_size $min --sm_restore_max_read_size $max --sm_restore_multiple_segments $maxseg"
            echo -n "Running for segsize=$segsize minread=$min maxread=$max maxsegs=$maxseg ... "
        else
            ARG="--segmentSize $i"
        fi
        
        echo -n "Running offline restore ... "
        # DEBUG_FLAGS="restore.cpp logarchiver.cpp" gdb -ex run --args \
        zapps restore -b tpcc -l /mnt/log-$DEVTYPE/log -a /mnt/archive-$DEVTYPE/archive \
            --bufsize 100000 --logsize 50000 --archWorkspace 2000 -q 100 -t 20 --eager --offline \
            --sm_backup_prefetcher_segments 64 \
            $ARG 1> out1.txt 2> out2.txt
            # --sm_restore_multiple_segments 64 --sm_restore_min_read_size 81920 --sm_restore_max_read_size 1048576 \
        echo "OK"


        echo -n "Gathering data ... "
        zapps agglog -i 1 -l /mnt/log-$DEVTYPE/log \
            -b restore_begin -e restore_end -t xct_end -t restore_segment \
            > agglog.txt 2> /dev/null
        echo "OK"

        kill $IOSTAT_PID > /dev/null 2>&1
        kill $MPSTAT_PID > /dev/null 2>&1

        mv *.txt $EXPDIR/
    done
done
