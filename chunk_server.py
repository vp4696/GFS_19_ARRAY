import socket
import threading
import os
import math
import pickle

class ChunkServer(object):
    
    def __init__(self, host, port):
        self.chunksize=2048
        self.chunk_servers = {}
        self.file_map = {}
        self.size = 0
        self.num_chunk_servers = 4
        self.filename = ''
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))


    


    
    


    
    

    


    def listen(self):
        self.sock.listen(5)
        while True:
            client, address = self.sock.accept()
            client.settimeout(60)
            threading.Thread(target = self.listenToClient,args = (client,address)).start()

    def listenToClient(self, client, address):
        msg=client.recv(1024).decode("utf-8")
        print(msg)

        

if __name__ == "__main__":
    while True:
        
        try:
            port_num = int(input("Enter the port number of the chunk_server"))
            break
        except ValueError:
            pass
    print("Chunk Server Running")
    ChunkServer('',port_num).listen()
