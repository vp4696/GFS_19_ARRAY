import socket
import threading
import os
import math
import pickle

class MasterServer(object):
    
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


    def numChunks(self,size):
        return int(math.ceil(size/self.chunksize))


    def allocChunks(self):
        i=0
        chunks=[]
        num_chunks = self.numChunks(self.size)
        for j in range(0,num_chunks):
            self.file_map[self.filename].append((j+1,i+1))
            chunks.append((j+1,i+1))
            i=(i+1)% self.num_chunk_servers
        #print(self.file_map)
        return chunks
    


    def write(self):
        if self.filename in self.file_map:
            pass
        self.file_map[self.filename] = []
        
        
        #num_chunks =self.numChunks(self.size)
        chunks = self.allocChunks()
        return chunks
    

    def upload(self):
        chunks = self.write()
        
        #print(chunks)
        for i in range(0,len(chunks)):
            k,l=chunks[i]
            #print(k)
            #print(l)
        return chunks


    def listen(self):
        self.sock.listen(5)
        while True:
            client, address = self.sock.accept()
            client.settimeout(60)
            threading.Thread(target = self.commonlisten,args = (client,address)).start()
           
           


   

    def listenToClient(self, client, address,filename,size):
        
        self.filename=filename
        self.size=int(size)
        print(self.filename)
        print(self.size)
        chunks=self.upload()
        data=pickle.dumps(chunks)
        print(type(data))
        client.send(data)
    

 
    def listentoChunk(self,client,address):
        s=client.recv(1024).decode("utf-8")
        print(s)

    def commonlisten(self,client,address):
        the_decision,filename,size=client.recv(1024).decode("utf-8").split(":")
        print(the_decision,type(the_decision))
        if(the_decision=="chunkserver"):
            self.listentoChunk(client,address)
        else:
            self.listenToClient(client,address,filename,size)
        

           
    
        

if __name__ == "__main__":
    while True:
        
        try:
            port_num = 7082
            break
        except ValueError:
            pass
    print("Master Server Running")
    MasterServer('',port_num).listen()
