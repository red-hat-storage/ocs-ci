#!/bin/bash

while true
do
for i in {1..125}
do
        dd if=/dev/zero of=/var/lib/www/html/mydir/emp$i bs=2048 count=1024 &
        echo /var/lib/www/html/mydir/emp$i
done

for i in {1..125}
do
        rm /var/lib/www/html/mydir/emp$i
        echo rm /var/lib/www/html/mydir/emp$i;
done
done
