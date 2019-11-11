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


    #listening to connections and after accepting makes a new thread for every connection
    def listen(self):
        self.sock.listen(5)
        while True:
            client, address = self.sock.accept()
            client.settimeout(60)
            threading.Thread(target = self.listenToClient,args = (client,address)).start()






    def connect_to_master(self):
        s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        s.connect((socket.gethostbyname('localhost'),7082))
        s.send(b"chunkserver:gobble:google")







    #The thread at work...accpeting each chunk from the client and storing in its directory

    def listenToClient(self, client, address):

        to_recv=client.recv(400).decode("utf-8")
        # chunk_server=client.recv(2048).decode("utf-8")
        # print(chunk_id)
        # print(chunk_server)
        
        # # print(to_recv)

        chunk_server_no,chunk_id,filenaming,dummy=to_recv.split(":")
        self.filesystem = os.getcwd()+"/"+str(chunk_server_no)
        print(self.filesystem)
        if not os.access(self.filesystem, os.W_OK):
            os.makedirs(self.filesystem)
        
        filename = self.filesystem+"/"+str(filenaming)+str(chunk_id)
        print(filename)
        with open(filename, "w") as f:
            chunks_recv=client.recv(2048)
            f.write(chunks_recv.decode("utf-8"))
        self.connect_to_master()
        

        

if __name__ == "__main__":
    while True:
        
        try:
            port_num = int(input("Enter the port number of the chunk_server"))
            break
        except ValueError:
            pass
    print("Chunk Server Running")
    ChunkServer('',port_num).listen()

    
