#!/bin/sh

SHA1=$(redis-cli SCRIPT LOAD "$(cat redimension.lua)")

redis-cli EVALSHA $SHA1 2 z h drop
redis-cli EVALSHA $SHA1 2 z h create 2 32
redis-cli EVALSHA $SHA1 2 z h index Josh 45 120000 
redis-cli EVALSHA $SHA1 2 z h index Pamela 50 110000 
redis-cli EVALSHA $SHA1 2 z h index Angela 30 125000
redis-cli EVALSHA $SHA1 2 z h query 40 50 100000 115000
