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
            threading.Thread(target = self.commonlisten,args = (client,address)).start()


    def connect_to_master(self,fname,chunkno):
        s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        s.connect((socket.gethostbyname('localhost'),7082))
        fname="chunkserver:"+fname+":"+chunkno
        s.send(bytes(fname,"utf-8"))
        cport=s.recv(2048).decode("utf-8")
        self.connectToChunk(cport)

    def connectToChunk(self,cport):
        s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        s.connect((socket.gethostbyname('localhost'),int(cport)))
        fname="chunkserver:"+"kuchbhi"+":"+"FROM--CHUNK:"+str(port_num)+":"
        fname=fname.ljust(400,'~')
        s.send(bytes(fname,"utf-8"))

    #The thread at work...accpeting each chunk from the client and storing in its directory

    def listenToChunk(self,client,address,one,two,three):
        print(one,two,three)

    def commonlisten(self,client,address):

        to_recv=client.recv(400).decode("utf-8")
        # print(to_recv)
        decision, one, two, three, dummy=to_recv.split(":")
        if(decision=="client"):
            chunk_server_no=one
            chunk_id=two
            filenaming=three
            self.listenToClient(client,address,chunk_server_no,chunk_id,filenaming)
        if(decision=="chunkserver"):
            # print(one,two,three)
            self.listenToChunk(client,address,one,two,three)

    def listenToClient(self, client, address, chunk_server_no, chunk_id, filenaming):

        #chunk_server_no,chunk_id,filenaming,dummy=to_recv.split(":")
        self.filesystem = os.getcwd()+"/"+str(chunk_server_no)
        # print(self.filesystem)
        if not os.access(self.filesystem, os.W_OK):
            os.makedirs(self.filesystem)
        
        filename = self.filesystem+"/"+str(filenaming)+"_"+str(chunk_id)
        # print(filename)
        with open(filename, "w") as f:
            chunks_recv=client.recv(2048)
            f.write(chunks_recv.decode("utf-8"))
        self.connect_to_master(filename,chunk_server_no)
        

if __name__ == "__main__":
    while True:
        try:
            port_num = int(input("Enter the port number of the chunk_server"))
            break
        except ValueError:
            pass
    print("Chunk Server Running")
    ChunkServer('',port_num).listen()

    
