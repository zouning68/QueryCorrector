#!/bin/bash

SCRIPT_NAME="server.py"
ps aux | grep "${SCRIPT_NAME}" | grep -v "grep" | awk '{print $2}' | xargs kill -9
