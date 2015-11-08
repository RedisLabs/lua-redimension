#!/bin/sh

SHA1=$(redis-cli SCRIPT LOAD "$(cat redimension.lua)")

redis-cli EVALSHA $SHA1 2 z h drop
redis-cli EVALSHA $SHA1 2 z h create 2 32
redis-cli EVALSHA $SHA1 2 z h update Josh 45 120000 
redis-cli EVALSHA $SHA1 2 z h update Pamela 50 110000 
redis-cli EVALSHA $SHA1 2 z h update George 41 100000 
redis-cli EVALSHA $SHA1 2 z h update Angela 30 125000
redis-cli EVALSHA $SHA1 2 z h query 40 50 100000 115000

redis-cli EVALSHA $SHA1 2 z h unindex_by_id Pamela
echo "After unindexing:"
redis-cli EVALSHA $SHA1 2 z h query 40 50 100000 115000

redis-cli EVALSHA $SHA1 2 z h update George 42 100000
echo "After updating:"
redis-cli EVALSHA $SHA1 2 z h query 40 50 100000 115000
