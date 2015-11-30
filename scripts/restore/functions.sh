#!/bin/bash

function die() { echo >&2 "$@"; exit 1; }

# dies if device is already mounted
function checkMounted
{
    if mount | grep -q $1; then
        die "Device $1 is already mounted!"
    fi
}

# returns UUID of a partition
function getUUID()
{
    # let udev rebuild by-uuid folder first
    udevadm trigger
    local uuid=$(blkid $1 | awk '{ print $2 }' | tr -d '"' | cut -c 6-)
    [ -h /dev/disk/by-uuid/$uuid ] || die "UUID of device $1 not found or invalid: $uuid"
    echo $uuid
}

# dies if device alread on /etc/fstab
# $1 = device file
# $2 = mountpoint (optional)
function checkFstab()
{
    if grep -q "^$1" /etc/fstab; then
        die "Device $1 already on fstab!"
    fi

    if [ -n "$2" ] && grep -q "^$2" /etc/fstab; then
        die "Mountpoint $2 already on fstab!"
    fi

    if local uuid=$(getUUID $1); then
        if [ -n "$uuid" ] && grep -q "^UUID=$uuid" /etc/fstab; then
            die "UUID of device $1 is already on fstab!"
        fi
    fi
}

# dies if partition contains a valid filesystem
function checkPartitionEmpty()
{
    local INFO=$(file -s $1 | cut -d" " -f 2)
    local errMsg="A filesystem or partition table was found in $1. Aborting."
    # file reutrns the string "data" if no filesystem is found
    [ $INFO == "data" ] || die $errMsg
    # we want to be super safe, so also check if blkid finds something
    [ -z $(blkid $1) ] || die $errMsg
}

# Allow a given user to eexecute a given command with sudo
# $1 = user
# $2 = path of executable to be added to sudoers
function setupSudo()
{
    # Allow zero user to run hdparm
    local sudo_line="$1 ALL = (root) NOPASSWD: $2"
    if grep -q "^$sudo_line\$" /etc/sudoers; then
        echo "sudoers file already configured for $2"
    else
        echo -n "Updating sudoers file for $1 to run $2 ..."
        echo $sudo_line >> /etc/sudoers
        echo "OK"
    fi
}

# Let given user execute some useful device-related commands with sudo
# $1 (optional) = user name, otherwise just the owner of the script
function setupPrivileges()
{
    if [ $UID -ne 0 ]; then
        die "Error: root privileges are required to setup IO devices!" >&2
    fi

    USER=$1
    if [ -z "$USER" ]; then
        USER=$(stat -c %U $0)
    fi

    if ! grep -q "^$USER" /etc/passwd; then
        die "Invalid user name: $USER"
    fi

    echo "Setting up sudo rights for user $USER"
    cp /etc/sudoers /etc/sudoers.backup
    setupSudo $USER /sbin/hdparm
    setupSudo $USER /sbin/blkid
    setupSudo $USER /sbin/mke2fs
    setupSudo $USER /usr/bin/file
    setupSudo $USER /bin/mount
    setupSudo $USER /bin/umount
    setupSudo $USER /bin/chmod
    setupSudo $USER /sbin/btrfs
}

# Format a device with the given options
function createFS
{
    local DEVICE=$1
    local OPTIONS=$2
    local FORCE=$3
    # run in subshell so that die command does not exit
    if ! $FORCE; then
        if ! errMsg=$(checkPartitionEmpty $DEVICE 2>&1); then
            read -p "Partition found in $DEVICE. Reformat? (DATA WILL BE LOST!!!) [y/n] " -n 1 -r; echo
            [[ $REPLY =~ ^[Yy]$ ]] || die $errMsg
        fi
    fi

    echo "Formatting $DEVICE"
    # local cmd=mke2fs
    # [ -x $(which $cmd) ] || die "Command not found: $cmd"

    # $cmd $MKFS_OPT $DEVICE
    sudo -n mke2fs $OPTIONS $DEVICE || die "Error formatting $DEVICE"
}

# Format a btrfs device with the given options
function createBTRFS
{
    local DEVICE=$1
    local OPTIONS=$2
    local FORCE=$3
    # run in subshell so that die command does not exit
    if ! $FORCE; then
        if ! errMsg=$(checkPartitionEmpty $DEVICE 2>&1); then
            read -p "Partition found in $DEVICE. Reformat? (DATA WILL BE LOST!!!) [y/n] " -n 1 -r; echo
            [[ $REPLY =~ ^[Yy]$ ]] || die $errMsg
        fi
    fi

    echo "Formatting $DEVICE"
    # local cmd=mke2fs
    # [ -x $(which $cmd) ] || die "Command not found: $cmd"

    # $cmd $MKFS_OPT $DEVICE
    sudo -n mkfs.btrfs $OPTIONS $DEVICE || die "Error formatting $DEVICE"
}
