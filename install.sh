#!/bin/sh
cat workers | while read line
do
    if [ "$line" = "-" ]; then
        echo "Skip $line"
    else
        ssh root@$line -n "rm -rf /lab1-test/ && mkdir /lab1-test/"
        echo "Copy data to $line"
        scp  /lab1-test/setup.py root@$line:/lab1-test/ && scp /lab1-test/manager root@$line:/lab1-test/ && scp /lab1-test/workers root@$line:/lab1-test/
        echo "Setup $line"
        ssh root@$line -n "cd /lab1-test/ && python3 setup.py && ntpdate time.nist.gov"
        echo "Finished config node $line"
    fi
done
