#!/bin/bash

# Update device paths here
declare -A HDD_DEVS
declare -A SSD_DEVS

# SSDs
SSD_DEVS[log]=/dev/sda1
SSD_DEVS[archive]=/dev/sdg1
SSD_DEVS[db]=/dev/sdh1
SSD_DEVS[backup]=/dev/sdi1

# HDDs
HDD_DEVS[log]=/dev/sdf1
HDD_DEVS[archive]=/dev/sdb1
HDD_DEVS[db]=/dev/sdd1
HDD_DEVS[backup]=/dev/sde1
