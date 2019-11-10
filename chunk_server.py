import socket
import threading
import os
import math
import pickle

class ChunkServer(object):

    
    
    def __init__(self, host, port):
        self.filesystem=""
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
        to_recv=client.recv(1024).decode("utf-8")
        chunk_server_no,chunk_id=to_recv.split(":")
        self.filesystem = os.getcwd()+"/"+str(chunk_server_no)
        if not os.access(self.filesystem, os.W_OK):
            os.makedirs(self.filesystem)
        #chunk_id=client.recv(1024).decode("utf-8")
        filename = self.filesystem+"/"+chunk_id
        with open(filename, "wb") as f:
            chunks_recv=client.recv(2048).decode("utf-8")
            f.write(chunks_recv)

        

if __name__ == "__main__":
    while True:
        
        try:
            port_num = int(input("Enter the port number of the chunk_server"))
            break
        except ValueError:
            pass
    print("Chunk Server Running")
    ChunkServer('',port_num).listen()
