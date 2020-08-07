#!/bin/bash
PID=$(ps aux | grep -i "[p]ython HistoricalArbCaller.py"  | awk '{print $2}')
lsof -p $PID +r 1 &>/dev/null
source /home/ubuntu/etfenv/bin/activate
cd /home/ubuntu/ETFAnalyzer/CalculateETFArbitrage/Helpers || exit
python Calculate_PNLdata_allETF.py
exit