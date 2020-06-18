#!/bin/bash
PID1=$(pgrep -f PerMinCaller.py)
PID2=$(pgrep -f TradesLive.py)
PID3=$(pgrep -f QuotesLive.py)
kill -9 $PID1 $PID2 $PID3