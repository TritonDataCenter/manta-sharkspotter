#!/usr/bin/bash

# kick_off_sharkspotter.sh
#
# start a bunch of sharkspotter processes to run in parallel.
# - each process has its own log file at './$moray_shard-sharkspotter.log'
#   e.g. 2.moray-sharkspotter.log
# - all of the sharkspotter processes will be configured to start at the
#   'beginning' of the table and will attempt to scan to the 'end' of the table
# - the caller can change the range of shards to scan by changing the min_shard
#   and max_shard variables below.

# The user can modify any of these to change how sharkspotter behaves
target_storage_id=
chunk_size=
min_shard=
max_shard=

# get the domain name of the moray url
domain=$(echo "$MORAY_URL" | awk -Fmoray '{print $2}' | awk -F:2020 '{print $1}' | cut -d. --complement -f 1)

start_sharkspotter()
{
  echo "$MORAY_URL: starting sharkspotter in background"
  command="./node/bin/node --abort-on-uncaught-exception sharkspotter.js -d $domain -s $target_storage_id -m $target_moray -c $chunk_size > $target_moray-sharkspotter.log &"
  echo "$MORAY_URL: executing $command"
  eval $command
}

for ((shard=$min_shard; shard<=$max_shard; shard++)); do
  target_moray="$shard.moray"
  MORAY_URL="tcp://$target_moray.$domain:2020"
  start_sharkspotter;
done;
