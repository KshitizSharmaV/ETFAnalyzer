#!/bin/bash
source /home/ubuntu/etfenv/bin/activate
cd /home/ubuntu/ETFAnalyzer/HoldingsDataScripts || exit
python HoldingsProcessCaller.py
exit
