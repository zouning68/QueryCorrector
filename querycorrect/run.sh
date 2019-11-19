#!/bin/bash

echo "run file: " $1

#ps -ef | grep $1 | grep -v grep | awk '{print $2}' | xargs kill -9

nohup python $1 &

