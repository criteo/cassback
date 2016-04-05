#!/bin/bash

kinit v.vanhollebeke@CRITEOIS.LAN -k -t ~/keytab

date=`date +%Y_%m_%d`

nodetool clearsnapshot

snapdir=$(nodetool snapshot| grep directory| awk '{print $NF}')
echo "Snapshot is $snapdir"

for dir in $(find /var/opt/cassandra/data -type d |grep snapshots/$snapdir); do
    kok=$(klist -l|grep v.vanhollebeke@CRITEOIS.LAN|grep -v Expired|wc -l)
    if [ $kok == 0 ]; then
        echo "Must renew Kerberos ticket"
        kinit v.vanhollebeke@CRITEOIS.LAN -k -t ~/keytab
    else
        echo "Kerberos ticket OK"
    fi
    keyspace=`echo $dir|awk -F\/ '{print $6}'`
    table=`echo $dir|awk -F\/ '{print $7}'`
    echo "Saving $keyspace $table"
    ./httpfs.sh /var/opt/cassandra/data/$keyspace/$table/snapshots/$snapdir tmp/cassandrabackups/prod/cstars02/$date/$HOSTNAME/$table

done

echo "FINISHED !!!!"
