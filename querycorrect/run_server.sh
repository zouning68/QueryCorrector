#!/bin/bash

SCRIPT_NAME=server.py

ps -ef | grep $SCRIPT_NAME | grep -v grep | awk '{print $2}' | xargs kill -9

nohup python $SCRIPT_NAME >> run.log 2>&1 &

