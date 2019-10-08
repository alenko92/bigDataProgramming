#!/bin/sh
cat workers | while read line
do
    if [ "$line" = "-" ]; then
        echo "Skip $line"
    else
        ssh root@$line -n "rm -rf /bigDataProgramming/ && mkdir /bigDataProgramming/"
        echo "Copy data to $line"
        scp  /bigDataProgramming/setup.py root@$line:/bigDataProgramming/ && scp /bigDataProgramming/manager root@$line:/bigDataProgramming/ && scp /bigDataProgramming/workers root@$line:/bigDataProgramming/
        echo "Setup $line"
        ssh root@$line -n "cd /bigDataProgramming/ && python3 setup.py && ntpdate time.nist.gov"
        echo "Finished config node $line"
    fi
done
