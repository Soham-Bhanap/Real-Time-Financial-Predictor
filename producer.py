import socket
import time
import random
import threading
import yfinance as yf

# The Master Node Tailscale IP
HOST = '100.110.200.107' 
PORT = 9999

# Initialize the TCP Socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # Prevents port lockouts
s.bind((HOST, PORT))
s.listen(5) # Upgraded to allow a queue of up to 5 teammates

print(f"[PRODUCER] Master Node Broadcasting on {HOST}:{PORT}...")

# Dynamic roster of connected teammates
clients = []
clients_lock = threading.Lock()

def accept_clients():
    """Background thread that constantly listens for new teammates joining."""
    while True:
        conn, addr = s.accept()
        print(f"\n[NETWORK] New Engine Connected: {addr}")
        with clients_lock:
            clients.append(conn)

# 1. Start the listener thread in the background
listener_thread = threading.Thread(target=accept_clients, daemon=True)
listener_thread.start()

# 2. The Main Thread: Market Data Generation & Broadcasting
while True:
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    try:
        ticker = yf.Ticker("TSLA")
        hist = ticker.history(period="5d")
        
        if not hist.empty and len(hist) >= 2:
            close_price = round(hist['Close'].iloc[-1], 2)
            volume = float(hist['Volume'].iloc[-1])
            ma_5 = round(hist['Close'].mean(), 2)
            vol_5 = round(hist['Close'].std(), 2)
        else:
            raise ValueError("Insufficient data.")
    except Exception:
        close_price = round(random.uniform(170.0, 180.0), 2)
        volume = random.uniform(100000, 500000)
        ma_5 = round(close_price + random.uniform(-2.0, 2.0), 2)
        vol_5 = round(random.uniform(1.0, 5.0), 2)

    payload = f"{timestamp},TSLA,{close_price},{volume},{ma_5},{vol_5}\n"
    
    # Safely broadcast to all connected laptops
    with clients_lock:
        active_clients = len(clients)
        print(f"Broadcasting to {active_clients} nodes: {payload.strip()}")
        
        # Iterate over a copy of the list to safely remove disconnected laptops
        for conn in list(clients):
            try:
                conn.sendall(payload.encode('utf-8'))
            except Exception:
                print("A consumer disconnected. Removing from roster.")
                clients.remove(conn)
        
    time.sleep(5)