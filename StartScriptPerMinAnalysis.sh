#!/bin/bash
source /home/ubuntu/etfenv/bin/activate
cd /home/ubuntu/ETFAnalyzer/ETFLiveAnalysisWS/ || exit
python PerMinCaller.py