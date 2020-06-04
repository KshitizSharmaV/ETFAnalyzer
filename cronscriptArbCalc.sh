#!/bin/bash
PID=$(pidof python)
while [ -e /proc/$PID ]; do
  sleep .6
done
source /home/ubuntu/etfenv/bin/activate
cd /home/ubuntu/ETFAnalysis/CalculateETFArbitrage/ || exit
cd /home/ubuntu/ETFAnalysis/CalculateETFArbitrage/ || exit
python Caller.py
