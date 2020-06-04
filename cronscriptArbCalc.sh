#!/bin/bash
PID=$(pidof python)
while [ -e /proc/$PID ]; do
  sleep .6
done
source /home/ubuntu/etfenv/bin/activate
cd /home/ubuntu/ETFAnalyzer/CalculateETFArbitrage/ || exit
python Caller.py
