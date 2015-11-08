#!/bin/sh

SHA1=$(redis-cli SCRIPT LOAD "$(cat redimension.lua)")

redis-cli EVALSHA $SHA1 2 z h fuzzy_test 4 100 1000
redis-cli EVALSHA $SHA1 2 z h fuzzy_test 3 100 1000
redis-cli EVALSHA $SHA1 2 z h fuzzy_test 2 1000 1000
