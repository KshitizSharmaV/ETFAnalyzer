#!/bin/bash
PID1=$(pgrep -f PolygonStocksWS3.py)
PID2=$(pgrep -f PerMinCaller.py)
kill -9 $PID1 $PID2