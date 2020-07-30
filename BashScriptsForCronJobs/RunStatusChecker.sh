#!/bin/bash
PID=$(ps aux | grep -i "[p]ython HoldingsProcessCaller.py"  | awk '{print $2}')
lsof -p $PID +r 1 &>/dev/null
source /home/ubuntu/etfenv/bin/activate
cd /home/ubuntu/ETFAnalyzer/CommonServices/ || exit
python StatusChecker.py
exit