#!/bin/bash
source /home/ubuntu/etfenv/bin/activate
cd /home/ubuntu/ETFAnalysis/ETFLiveAnalysisWS/ || exit
python PolygonStocksWS3.py
