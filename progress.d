#!/usr/sbin/dtrace -Cs

/*
 * progress.d - get the approximate progress of sharkspotter processes.
 *
 * User provides a string argument on the CLI that includes the shard number
 *  to get progress from:
 *
 *	./progress.d '"2"'
 */

#pragma D option strsize=4k
#pragma D option quiet
#pragma D option zdefs

BEGIN
{
	printf("waiting for records from moray...\n");
}

bunyan*:::log-debug
/
do_print &&
json(copyinstr(arg0), "msg") == "record received" &&
strtok(json(copyinstr(arg0), "moray"), ".") == $1
/
{
	js = copyinstr(arg0);
	moray = json(js, "moray");
	end_id = strtoll(json(js, "end_id"));
	begin_id = strtoll(json(js, "begin_id"));
	last_id = strtoll(json(js, "last_id"));
	records_seen = strtoll(json(js, "records_seen"));

	progress = (begin_id + records_seen - 1);
	shardnum = strtok(moray, ".moray");

	printf("%s.moray: %d%% [ %d / %d ]\n",
	    shardnum,
	    (progress * 100) / last_id,
	    progress,
	    last_id);
	do_print = 0;
}

bunyan*:::log-info
/
json(copyinstr(arg0), "msg") == "sharkspotter: done"
/
{
	printf("sharkspotter completed");
	exit(0);
}

tick-1s
{
	do_print = 1;
}
