#!/bin/bash
PID=$(pgrep -f Caller.py)
while [ -e /proc/$PID ]; do
  sleep .6
done
source /home/ubuntu/etfenv/bin/activate
cd /home/ubuntu/ETFAnalyzer/CalculateETFArbitrage/ || exit
python Calculate_PNLdata_allETF.py
exit