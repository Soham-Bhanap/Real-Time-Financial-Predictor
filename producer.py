import socket
import time
import random
import pandas as pd
import yfinance as yf

HOST = '100.110.200.107' 
PORT = 9999

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((HOST, PORT))
s.listen(1)

print(f"[PRODUCER] Master Node listening on {HOST}:{PORT}...")
conn, addr = s.accept()
print(f"Connected to Spark Engine at {addr}")

while True:
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    try:
        # Fetch the last 5 days of data to calculate rolling metrics
        ticker = yf.Ticker("TSLA")
        hist = ticker.history(period="5d")
        
        if not hist.empty and len(hist) >= 2:
            close_price = round(hist['Close'].iloc[-1], 2)
            volume = float(hist['Volume'].iloc[-1])
            ma_5 = round(hist['Close'].mean(), 2)
            vol_5 = round(hist['Close'].std(), 2)
        else:
            raise ValueError("Insufficient data for rolling metrics.")
            
    except Exception as e:
        # Synthetic fallback to keep the pipeline alive if Yahoo API blocks you
        close_price = round(random.uniform(170.0, 180.0), 2)
        volume = random.uniform(100000, 500000)
        ma_5 = round(close_price + random.uniform(-2.0, 2.0), 2)
        vol_5 = round(random.uniform(1.0, 5.0), 2)

    # The New Data Contract Payload
    payload = f"{timestamp},TSLA,{close_price},{volume},{ma_5},{vol_5}\n"
    print(f"Broadcasting: {payload.strip()}")
    
    try:
        conn.sendall(payload.encode('utf-8'))
    except BrokenPipeError:
        print("Consumer disconnected.")
        break
        
    time.sleep(5)