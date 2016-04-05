#!/bin/sh

BASE='http://0.httpfs.hpc.criteo.prod:14000/webhdfs/v1'
#BASE='http://httpfs.pa4.hpc.criteo.prod:14000'

IN=$1
OUT=$2

echo "Creating destination directory: $OUT"
curl --negotiate -u : "$BASE/$OUT?op=MKDIRS&permission=0777" -X PUT -s > /dev/null

for p in $(find $IN -type f)
do
    f=$(basename $p)
    echo "$IN/$f"

    # Create file
    dest=$(curl --negotiate -u : "$BASE/$OUT/$f?op=CREATE&overwrite=true&permission=0777" -i -X PUT -s | grep Location | tail -n1 | cut -d\  -f2 | tr -d '\r\n')
    [ $? != 0 ] && echo "ERROR"

    echo "DEST IS ${dest}"

    # Upload file
    curl --negotiate -u : "$dest" -i -X PUT -T "$IN/$f" -H 'Content-Type: application/octet-stream' > /dev/null
    [ $? != 0 ] && echo "ERROR"

done
