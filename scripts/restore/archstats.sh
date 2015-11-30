#!/bin/bash

# number of buckets to consider as one unit on the x axis
# (one bucket is by default 128 pages)
BUCKET_COUNT=1

AWK_SCRIPT='
    BEGIN {
        prev=-1;
        accum=0;
    }
    {
        accum += $10;
        if ($4 % '"$BUCKET_COUNT"' == 0) {
            totals[$4] += accum;
            accum = 0;
        }
    }
    END {
        i = 0;
        for(t in totals) {
            print i++, totals[t];
        }
    }
'

if [ ! -f logstats.txt ]; then
    zapps logstats -a -l paths/archive > logstats.txt
fi
awk "$AWK_SCRIPT" logstats.txt
