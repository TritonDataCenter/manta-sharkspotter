# sharkspotter

Sharkspotter is a program that scans Postgres for Manta objects that should
reside on a specific shark. This was made to be a quick work around for the
absence of online storage auditing required by large Manta deployments.

## Installing

```
$ npm install
```

## Running

Invoke sharkspotter from the CLI with a few required arguments.


Description of the flags:
```
  -b, --begin		manta relation _id to start search
			from
  -e, --end		manta relation _id to end search at
  -d, --domain		domain name of manta services
			e.g. us-east.joyent.us
  -m, --moray		moray shard to search
			e.g. 2.moray
  -s, --shark		shark to search objects in moray for
			e.g. 3.stor
  -c, --chunk-size	number of objects to search in each PG
			query
			default is 10000
```

Currently all flags must be provided except for the `-c` flag.

Here's an example from a development setup:
```
$ node --abort-on-uncaught-exception sharkspotter.js -b 0 -e 80000 -d walleye.kkantor.com -m 2.moray -s 3.stor -c 20000
```

The process outputs bunyan-style logs, so feel free to pipe the command into
bunyan, to a file to look at later, or using `tee(1)`, both!

This command will start the search from the 'beginning' of the manta table on
the 2.moray shard ending at the 80000th record in chunks of 20000. The program
will search for objects that should exist on 3.stor.

Matching objects are written to a file in the current directory. The file name
follows the pattern `moray_shard.pid.out`.

The output format folows the form:
```
owner_uuid object_uuid shark_1 shark_2 ... shark_N
```
One object is listed on each line.

Here's an example of the output using the above example invocation:
```
$ tail 2.moray.walleye.kkantor.com.14013.out
0864994a-6ef0-e5a5-a86c-e64790f5e90c 8052f0eb-16da-ca6e-a94c-e95da3419f3c 3.stor.walleye.kkantor.com 1.stor.walleye.kkantor.com
0864994a-6ef0-e5a5-a86c-e64790f5e90c a9a675a1-6d00-e9c7-a585-8d87b318877e 1.stor.walleye.kkantor.com 3.stor.walleye.kkantor.com
0864994a-6ef0-e5a5-a86c-e64790f5e90c 6b278fd0-c353-6c28-ba79-e365ceb5484c 3.stor.walleye.kkantor.com 1.stor.walleye.kkantor.com
0864994a-6ef0-e5a5-a86c-e64790f5e90c 0d15642e-3081-e568-e59a-a42e8ee879e9 3.stor.walleye.kkantor.com 1.stor.walleye.kkantor.com
0864994a-6ef0-e5a5-a86c-e64790f5e90c 7c586264-de59-4af5-fe45-d9ead9567e71 3.stor.walleye.kkantor.com 1.stor.walleye.kkantor.com
0864994a-6ef0-e5a5-a86c-e64790f5e90c 236daedb-b0d8-6391-c970-a916087e07d8 3.stor.walleye.kkantor.com 1.stor.walleye.kkantor.com
0864994a-6ef0-e5a5-a86c-e64790f5e90c cd6db21f-fd01-482b-f369-e00678f9cebb 3.stor.walleye.kkantor.com 2.stor.walleye.kkantor.com
0864994a-6ef0-e5a5-a86c-e64790f5e90c 2cf1f699-9cb3-c05a-b3aa-8c43dd207cc7 3.stor.walleye.kkantor.com 2.stor.walleye.kkantor.com
0864994a-6ef0-e5a5-a86c-e64790f5e90c f3bc9886-bc53-680f-dfe6-dada6663c53a 3.stor.walleye.kkantor.com 1.stor.walleye.kkantor.com
```
