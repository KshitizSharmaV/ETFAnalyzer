#!/bin/bash
source /home/ubuntu/etfenv/bin/activate
cd /home/ubuntu/ETFAnalyzer/ || exit
python ProcessCaller.py
