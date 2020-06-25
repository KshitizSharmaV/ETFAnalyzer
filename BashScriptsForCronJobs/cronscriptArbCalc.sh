#!/bin/bash
PID=$(ps aux | grep -i "[p]ython ProcessCaller.py"  | awk '{print $2}')
while [ -e /proc/$PID ]; do
  sleep .6
done
source /home/ubuntu/etfenv/bin/activate
cd /home/ubuntu/ETFAnalyzer/CalculateETFArbitrage/ || exit
python Caller.py
