#!/bin/bash

source functions.sh || (echo "functions.sh not found!"; exit)

set -e

if [ $UID -ne 0 ]; then
    die "Error: root privileges are required to setup IO devices!" >&2
fi

setupPrivileges
