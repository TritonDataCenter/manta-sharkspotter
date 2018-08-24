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
			default is 0
  -e, --end		manta relation _id to end search at
			default is the larger of max(_id) and
      			max(_idx)
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

Here's an example from a development setup:
```
$ node --abort-on-uncaught-exception sharkspotter.js -d walleye.kkantor.com -m 2.moray -s 3.stor -c 20000
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


## Monitoring

The bundled DTrace script, `progress.d`, can be used to watch the progress of
one sharkspotter process. You must pass in a quoted shard number as the
sole argument:

```
$ ./progress.d '"2"'
waiting for records from moray...
2.moray: 0% [ 0 / 134222 ]
2.moray: 0% [ 5 / 134222 ]
2.moray: 3% [ 4375 / 134222 ]
2.moray: 6% [ 8402 / 134222 ]
2.moray: 9% [ 12374 / 134222 ]
2.moray: 12% [ 17000 / 134222 ]
2.moray: 16% [ 22000 / 134222 ]
2.moray: 19% [ 26001 / 134222 ]
2.moray: 22% [ 30662 / 134222 ]
2.moray: 25% [ 34378 / 134222 ]
2.moray: 28% [ 38717 / 134222 ]
2.moray: 31% [ 42263 / 134222 ]
2.moray: 34% [ 46641 / 134222 ]
2.moray: 37% [ 50719 / 134222 ]
2.moray: 40% [ 54265 / 134222 ]
2.moray: 42% [ 57222 / 134222 ]
2.moray: 45% [ 60745 / 134222 ]
2.moray: 48% [ 64442 / 134222 ]
2.moray: 51% [ 68598 / 134222 ]
2.moray: 54% [ 73000 / 134222 ]
2.moray: 57% [ 77359 / 134222 ]
2.moray: 61% [ 82000 / 134222 ]
2.moray: 63% [ 85475 / 134222 ]
2.moray: 67% [ 90002 / 134222 ]
2.moray: 69% [ 93538 / 134222 ]
2.moray: 72% [ 97365 / 134222 ]
2.moray: 75% [ 101257 / 134222 ]
2.moray: 78% [ 105000 / 134222 ]
2.moray: 81% [ 109000 / 134222 ]
2.moray: 83% [ 112288 / 134222 ]
2.moray: 86% [ 116053 / 134222 ]
2.moray: 89% [ 120000 / 134222 ]
2.moray: 91% [ 123428 / 134222 ]
2.moray: 94% [ 127451 / 134222 ]
2.moray: 97% [ 131337 / 134222 ]
sharkspotter completed
```
