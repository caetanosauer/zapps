#!/bin/bash

source functions.sh || (echo "functions.sh not found!"; exit)
source devlist.sh || die "Device list not found"

# EDIT THESE VARIABLES ONLY
USE_SSD=true
USE_HDD=false
MOUNTOPTS="noatime,noexec,noauto,nodev,nosuid"
# END EDIT

FORCE=false
if [[ "$1" == "--force" ]]; then
    FORCE=true
fi

set -e

# Get username of script owner
USER=$(stat -c %U $0)

# Arguments:
# $1 = name of associative array as declared in devlist.sh
# $2 = prefix string "hdd" or "ssd"
function reformatDevs()
{
    declare -n DEVS=$1
    for d in "${!DEVS[@]}"; do
        devpath=${DEVS[$d]} 
        mountpath=/mnt/$d-$2

        echo "Processing device $devpath on $2"

        # Unmount
        if mount | grep -q "$devpath" -; then
            echo "Unmounting $devpath"
            sudo -n umount $devpath
        fi

        # Format and mount
        mkdir -p $mountpath
        if [[ "$d" == "db" ]]; then
            createBTRFS $devpath "-f" $FORCE
            sudo -n mount -o "$MOUNTOPTS,user_subvol_rm_allowed" $devpath $mountpath

        else
            createFS $devpath "-t ext3 -O ^has_journal -q" $FORCE
            sudo -n mount -o $MOUNTOPTS $devpath $mountpath
        fi

        # Create links
        if [[ "$d" == "db" ]]; then
            btrfs subvolume create $mountpath/old
        fi

        chown -R $USER $mountpath
    done
}

if $USE_HDD; then 
    reformatDevs "HDD_DEVS" "hdd";
fi
if $USE_SSD; then 
    reformatDevs "SSD_DEVS" "ssd";
fi
