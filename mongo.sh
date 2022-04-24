#!/bin/bash
products=(
    'BTC-USDT' 'BTC-USD-PERP'
    'ETH-USDT' 'ETH-USD-PERP'
    'SOL-USDT' 'SOL-USD-PERP'
    'DOGE-USDT' 'DOGE-USD-PERP'
    'SLP-USD' 'SLP-USD-PERP'
    'XRP-USDT' 'XRP-USD-PERP'
    'FTT-USDT' 'FTT-USD-PERP'
    'BNB-USDT' 'BNB-USD-PERP'
    'AVAX-USDT' 'AVAX-USD-PERP'
    'DOT-USDT' 'DOT-USD-PERP'
    'GALA-USD' 'GALA-USD-PERP'
)
# source ~/.zshrc
# pyenv activate py3.8
# awk '{print $2}' pid/pid.txt | xargs kill -9

for product in ${products[@]}
do
    # echo $product
    # nohup python3 -u ftx_ticker_trades.py -p $product 2>&1 >>output.txt | xargs >> pid.txt &
    nohup python -u ftx_ticker_trades.py -p $product -l log/"feedhandler_$product" 2>&1 >output/"output_$product.txt" & 
    echo "$product $!" >> pid/pid.txt
done