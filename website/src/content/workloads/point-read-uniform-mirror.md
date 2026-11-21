Runs closed-loop point reads against uniformly selected records in the golden
database with compacted SSTs mirrored to local disk via `slatedb-mirror`. Every
read is expected to find a value.
