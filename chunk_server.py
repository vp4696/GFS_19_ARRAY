import socket
import threading
import os
import math
import sys
import pickle

class ChunkServer(object):
    
    def __init__(self, host, port):
        self.filesystem=""
        self.myChunkDir=""
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


    def connect_to_master(self,fname,chunk_id,filename):
        s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        s.connect((socket.gethostbyname('localhost'),7082))
        filenameToCS=fname
        fname="chunkserver:"+fname+":"+chunk_id+":dummyData"
        s.send(bytes(fname,"utf-8"))
        cport=s.recv(2048).decode("utf-8")
        self.connectToChunk(cport,filenameToCS,chunk_id,filename)

    def connectToChunk(self,cport,filenameToCS,chunk_id,filename):
        s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        s.connect((socket.gethostbyname('localhost'),int(cport)))
        fname="chunkserver:"+"kuchbhi:"+filenameToCS+":"+chunk_id+":"+str(port_num)+":"
        fname=fname.ljust(400,'~')
        s.send(bytes(fname,"utf-8"))

        f1=open(filename,'rb')
        data=f1.read(2048)
        s.send(data)


    #The thread at work...accpeting each chunk from the client and storing in its directory

    def listenToChunk(self,client,address,one,two,three):
        # print(one,two,three)
        path=self.myChunkDir+"/"+str(one)+"_"+str(two)
        # print(path)
        with open(path, 'wb') as f:
            c_recv=client.recv(2048)
            # f.write(c_recv.decode("utf-8"))
            f.write(c_recv)


    def sendToClient(self,client,address,one,two,three):
        path=self.myChunkDir+"/"+str(three)+"_"+str(two)
        # print(path)
        with open(path, 'rb') as f:
            data=f.read(2048)
            client.send(data)


    def commonlisten(self,client,address):

        to_recv=client.recv(400).decode("utf-8")
        # print(to_recv)
        decision, whatToDo, one, two, three, dummy=to_recv.split(":")
        if(decision=="client"):
            if(whatToDo=="upload"):
                chunk_server_no=one
                chunk_id=two
                filenaming=three
                self.listenToClient(client,address,chunk_server_no,chunk_id,filenaming)
            if(whatToDo=="download"):
                chunk_server_no=one
                chunk_id=two
                filenaming=three
                self.sendToClient(client,address,chunk_server_no,chunk_id,filenaming)

        elif(decision=="chunkserver"):
            # print(one,two,three)
            self.listenToChunk(client,address,one,two,three)

    def listenToClient(self, client, address, chunk_server_no, chunk_id, filenaming):

        #chunk_server_no,chunk_id,filenaming,dummy=to_recv.split(":")
        self.filesystem = os.getcwd()+"/"+str(chunk_server_no)
        self.myChunkDir=self.filesystem
        # print(self.filesystem)
        if not os.access(self.filesystem, os.W_OK):
            os.makedirs(self.filesystem)
        
        filename = self.filesystem+"/"+str(filenaming)+"_"+str(chunk_id)
        # print(filename)
        with open(filename, 'wb') as f:
            chunks_recv=client.recv(2048)
            # f.write(chunks_recv.decode("utf-8"))
            f.write(chunks_recv)
        self.connect_to_master(filenaming,chunk_id,filename)
        

if __name__ == "__main__":
    while True:
        try:
            # port_num = int(input("Enter the port number of the chunk_server"))
            port_num = int(sys.argv[1])
            # print(port_num)
            break
        except ValueError:
            pass
    print("Chunk Server Running")
    ChunkServer('',port_num).listen()

    
