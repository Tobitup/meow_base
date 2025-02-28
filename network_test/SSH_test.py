import paramiko
import time
import socket
import threading

PORT = 10001
REMOTE_IP = "172.20.28.89"  
USERNAME = "rlf574"   
PASSWORD = "qkh25yvb"

def start_listener():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((REMOTE_IP, int(PORT)))
        server_socket.listen(5)                                     # listen mode 1 (1 connection(s) are kept waiting if the server is busy)
        print(f"Listening on port {PORT}...")
        
        conn, addr = server_socket.accept()
        print(f"Connection from {addr} has been established!")
        while True:
            data = conn.recv(1024)
            if not data:
                break
            print(data.decode())

# Start the listener thread
listening_thread = threading.Thread(target=start_listener, daemon=True)
listening_thread.start()

# SSH Connection
print("SSHD")
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
try:
    client.connect(REMOTE_IP, username=USERNAME, password=PASSWORD)
 
    print("Connected")

    command = "cd /home/rlf574/meow_base && python3 examples/hello_world.py\n"
    stdin, stdout, stderr = client.exec_command(command)

    # Read from stdout/stderr to see if the remote script prints anything
    time.sleep(10)
    out = stdout.read().decode()
    err = stderr.read().decode()
    print(f"[SSH] STDOUT: {out}")
    print(f"[SSH] STDERR: {err}")
    
except Exception as e:
    print(f"SSH Connection Error: {e}")

