redimension.lua
===

A port of [Redimension](https://github.com/antirez/redimension) to Redis Lua with a semi-Redis API. Developed 100% without a debugger.

Divergences
===

* Indices always use a Sorted Set (for ranges) and a Hash (for id lookups).

Known issues
===

* Can't index elements named _dim and _prec
* Can't deal with elements that have colons (':')
* Lua's integers are 32 bit, could lead to breakage somewhen (`2^exp`...)

Usage
===

Use `EVAL`, `EVALSHA` or `redis-cli --eval` to run redimension.lua.

The script requires two key names and at least one argument, as follows:

* KEYS[1] - the index sorted set key
* KEYS[2] - the index hash key
* ARGV[1] - the command

The command may be one of the following:

* create - create an index with ARGV[2] as dimension and ARGV[3] as precision',
* drop          - drops an index
* index         - index an element ARGV[2] with ARGV[3]..ARGV[3+dimension] values
* unindex       - unindex an element ARGV[2] with ARGV[3]..ARGV[3+dimension] values
* unindex_by_id - unindex an element by id ARGV[2]
* update        - update an element ARGV[2] with ARGV[3]..ARGV[3+dimension] values
* query         - query using ranges ARGV[2], ARGV[3]..ARGV[2+dimension-1], ARGV[2+dimension]
* fuzzy_test    - fuzzily tests the library on ARGV[2] dimension with ARGV[3] items using ARGV[4] queries

Testing
===

Fuzzy-ish testing is implemented inline - use the `fuzzy_test` command to invoke.

License
===

The code is released under the BSD 2 clause license.
