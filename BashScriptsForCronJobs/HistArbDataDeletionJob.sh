#!/bin/bash
source /home/ubuntu/etfenv/bin/activate
cd /home/ubuntu/ETFAnalyzer/CalculateETFArbitrage/Helpers/ || exit
python DeleteScriptOldQuotesTrades.py
exit