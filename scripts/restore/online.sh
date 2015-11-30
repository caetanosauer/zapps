#!/bin/bash

# Arguments
# $1 scale factor
# $2 duration folder from which to copy log
# $3 list of segment sizes for which to run experiment
#    e.g., "128 1024 8192"

set -e

SF=$1
DURATION=$2
BUFSIZES=$3

RUN_GDB=false
if [[ "$4" == "--debug" ]]; then
    RUN_GDB=true
fi

DEVTYPE="ssd"
SEGSIZE=16384

[ "$#" -ge 3 ] || (echo "Wrong arguments"; exit 1)

MAX_THREADS=24
THREADS=$(( MAX_THREADS > SF ? SF : MAX_THREADS ))

SRCDIR=/mnt/snap/bench/"$DURATION"s/tpcc-$SF
[ -d $SRCDIR ] || (echo "Folder not found: $SRCDIR"; exit 1)

for i in $BUFSIZES; do
    echo "=== RUNNING FOR $i ==="

    EXPDIR=~/experiments/online/restore-$i
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
    [ -d $SRCDIR/archive ] || (echo "Archive not found in $SRCDIR/archive_$a"; exit 1)

    echo -n "Copying log archive and recovery log ... "
    # Reformat log on every run (CS TODO)
    if mount | grep -q "/dev/sda1" -; then
        sudo -n umount /dev/sda1
    fi
    sudo -n mke2fs -O ^has_journal -q /dev/sda1
    sudo -n mount -o "noatime,noexec,noauto,nodev,nosuid" /dev/sda1 /mnt/log-$DEVTYPE
    sudo -n chmod 777 /mnt/log-$DEVTYPE
    sudo -n fstrim /mnt/log-$DEVTYPE

    rsync -a --delete $SRCDIR/archive/ /mnt/archive-$DEVTYPE/archive/
    rm -f /mnt/log-$DEVTYPE/log/*
    rsync -a --delete $SRCDIR/log/ /mnt/log-$DEVTYPE/log/
    echo "OK"

    # COPY BACKUP
    [ -f $SRCDIR/backup ] || (echo "Backup file not found in $SRCDIR"; exit 1)

    echo -n "Copying backup file ... "
    rsync -a $SRCDIR/backup /mnt/backup-$DEVTYPE/backup
    ln -fs /mnt/backup-$DEVTYPE/backup paths/backup
    echo "OK"

    # exit

    iostat -dmtx 1 > iostat.txt 2> /dev/null &
    IOSTAT_PID=$!

    mpstat 1 > mpstat.txt 2> /dev/null &
    MPSTAT_PID=$!

    CMD="zapps restore -b tpcc -l /mnt/log-$DEVTYPE/log -a /mnt/archive-$DEVTYPE/archive \
        --bufsize $i --logsize 4000000 --archWorkspace 1000 -q $SF -t $THREADS --eager true \
        --duration 600 --failDelay 600 --singlePass --segmentSize $SEGSIZE \
        --warmup 10 --sm_restore_reuse_buffer true --sm_shutdown_clean false \
        --sm_restore_max_read_size 1048576 --sm_restore_preemptive true --instant false"

#     CMD="zapps kits -b tpcc -l /mnt/log-$DEVTYPE/log -a /mnt/archive-$DEVTYPE/archive \
#         --bufsize $i --logsize 4000000 --archWorkspace 1000 -q $SF -t $THREADS --eager \
#         --duration 600 --warmup 10 --sm_shutdown_clean false"

    echo -n "Running online restore ... "
    if $RUN_GDB; then
        DEBUG_FLAGS="restore.cpp log_spr.cpp" gdb -ex run --args $CMD
    else
        $CMD 1> out1.txt 2> out2.txt
    fi

    echo -n "Gathering data ... "
    zapps agglog -i 1 -l /mnt/log-$DEVTYPE/log \
        -t restore_begin -t restore_end -t xct_end -t restore_segment \
        -t page_read -t page_write \
        > agglog.txt 2> /dev/null
    echo "OK"

    kill $IOSTAT_PID > /dev/null 2>&1
    kill $MPSTAT_PID > /dev/null 2>&1

    mv *.txt $EXPDIR/
done
