import socket
import threading
import os
import math
import pickle

chunk_port=[6467,6468,6469,6470]

class MasterServer(object):
    
    def __init__(self, host, port):
        self.chunksize=2048
        self.chunk_servers = {}
        self.file_map = {}
        self.size = 0
        self.num_chunk_servers = 4
        self.chunk_servers_info={}
        self.replica = {}
        self.fileinfo={}                        #{(filename, # of chunks)}
        self.replica={}
        self.filename = ''
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        

    def numChunks(self,size):
        return int(math.ceil(size/self.chunksize))


    def chunkserverinfo(self,file_map):
        self.chunk_servers_info ={1:[],2:[],3:[],4:[]}
        for i in file_map.keys():
            for j in range(len(file_map[i])):
                self.chunk_servers_info[file_map[i][j][1]].append((i,file_map[i][j][0]))
        # print(self.chunk_servers_info)

        i=0
        a1=len(self.chunk_servers_info[1])
        a2=len(self.chunk_servers_info[2])
        a3=len(self.chunk_servers_info[3])
        a4=len(self.chunk_servers_info[4])
        maximum = max([a1,a2,a3,a4])
        while i < maximum:
            l1=len(self.chunk_servers_info[1])
            l2=len(self.chunk_servers_info[2])
            l3=len(self.chunk_servers_info[3])
            l4=len(self.chunk_servers_info[4])
            min1 = min([l1,l2,l3,l4])
            # print(min1)
            if i <= a1-1:
                for m in self.chunk_servers_info.keys():
                    if len(self.chunk_servers_info[m]) == min1 and (self.chunk_servers_info[1][i] not in self.chunk_servers_info[m]):
                        #print(min1," ",m)
                        self.chunk_servers_info[m].append(self.chunk_servers_info[1][i])
                        break
            l1=len(self.chunk_servers_info[1])
            l2=len(self.chunk_servers_info[2])
            l3=len(self.chunk_servers_info[3])
            l4=len(self.chunk_servers_info[4])
            min1 = min([l1,l2,l3,l4])
            # print(min1)
            if i <= a2-1:
                for m in self.chunk_servers_info.keys():
                    if len(self.chunk_servers_info[m]) == min1 and (self.chunk_servers_info[2][i] not in self.chunk_servers_info[m]):
                        #print(min1," ",m)
                        self.chunk_servers_info[m].append(self.chunk_servers_info[2][i])
                        break
            l1=len(self.chunk_servers_info[1])
            l2=len(self.chunk_servers_info[2])
            l3=len(self.chunk_servers_info[3])
            l4=len(self.chunk_servers_info[4])
            min1 = min([l1,l2,l3,l4])
            # print(min1)
            if i <= a3-1:
                for m in self.chunk_servers_info.keys():
                    if len(self.chunk_servers_info[m]) == min1 and (self.chunk_servers_info[3][i] not in self.chunk_servers_info[m]):
                        #print(min1," ",m)
                        self.chunk_servers_info[m].append(self.chunk_servers_info[3][i])
                        break
            l1=len(self.chunk_servers_info[1])
            l2=len(self.chunk_servers_info[2])
            l3=len(self.chunk_servers_info[3])
            l4=len(self.chunk_servers_info[4])
            min1 = min([l1,l2,l3,l4])
            # print(min1)
            if i <= a4-1:
                for m in self.chunk_servers_info.keys():
                    if len(self.chunk_servers_info[m]) == min1 and (self.chunk_servers_info[4][i] not in self.chunk_servers_info[m]):
                        #print(min1," ",m)
                        self.chunk_servers_info[m].append(self.chunk_servers_info[4][i])
                        break
            i+=1
        
        # print("The below is the chunk_server_info data structure")
        # print(self.chunk_servers_info)


        i=0
        a1=len(self.chunk_servers_info[1])
        a2=len(self.chunk_servers_info[2])
        a3=len(self.chunk_servers_info[3])
        a4=len(self.chunk_servers_info[4])
        max1=max(a1,a2,a3,a4)
        while i < max1:
            if i <= a1-1:
                if self.chunk_servers_info[1][i] not in self.replica.keys():
                    self.replica[self.chunk_servers_info[1][i]] = [1]
                else:
                    self.replica[self.chunk_servers_info[1][i]].append(1)

            if i <= a2-1:
                if self.chunk_servers_info[2][i] not in self.replica.keys():
                    self.replica[self.chunk_servers_info[2][i]] = [2]
                else:
                    self.replica[self.chunk_servers_info[2][i]].append(2)

            if i <= a3-1:
                if self.chunk_servers_info[3][i] not in self.replica.keys():
                    self.replica[self.chunk_servers_info[3][i]] = [3]
                else:
                    self.replica[self.chunk_servers_info[3][i]].append(3)

            if i <= a4-1:
                if self.chunk_servers_info[4][i] not in self.replica.keys():
                    self.replica[self.chunk_servers_info[4][i]] = [4]
                else:
                    self.replica[self.chunk_servers_info[4][i]].append(4)
            i+=1
        # print("The below is the self.replica data structure")
        # print(self.replica)


    def allocChunks(self):
        i=0
        chunks=[]
        num_chunks = self.numChunks(self.size)
        for j in range(0,num_chunks):
            self.file_map[self.filename].append((j+1,i+1))
            chunks.append((j+1,i+1))
            i=(i+1)% self.num_chunk_servers
        #print(self.file_map)
        self.chunkserverinfo(self.file_map)
        return chunks
    


    def write(self):
        if self.filename in self.file_map:
            pass
        self.file_map[self.filename] = []
        chunks = self.allocChunks()

        num_chunks = self.numChunks(self.size)
        self.fileinfo[self.filename]=num_chunks
        # print(self.fileinfo)                              #{'filename':# of chunks}
        
        return chunks
    

    def upload(self):
        chunks = self.write()
        #print(chunks)
        for i in range(0,len(chunks)):
            k,l=chunks[i]
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
        # print(self.filename)
        # print(self.size)
        chunks=self.upload()
        self.file_map={}
        data=pickle.dumps(chunks)
        # print(type(data))
        client.send(data)
    
    def listentoChunk(self,client,address,filename,chunkNo):
        # print(filename," FROM CHUNK-SERVER ",chunkNo)
        chunkNo=int(chunkNo)
        # cport=chunk_port[(chunkNo)%4]
        cport=chunk_port[self.replica[(filename,chunkNo)][1]-1]
        cport=str(cport)
        client.send(bytes(cport,"utf-8"))


    def commonlisten(self,client,address):
        the_decision,one,two,three=client.recv(1024).decode("utf-8").split(":")
        # print(the_decision,type(the_decision))
        if(the_decision=="chunkserver"):
            filename=one
            chunk__no=two
            self.listentoChunk(client,address,filename,chunk__no)
        else:
            if(one=="upload"):
                filename=two
                size=three
                self.listenToClient(client,address,filename,size)
            elif(one=="download"):
                filename=two
                count=self.fileinfo.get(filename)
                # print(count)
                res=[]
                while count > 0:
                    res.append([count,self.replica[(filename,count)][0]])
                    count=count-1
                res.reverse()    
                # print(res)    
                data=pickle.dumps(res)
                # print(type(data))
                client.send(data)



if __name__ == "__main__":
    while True:
        
        try:
            port_num = 7082
            break
        except ValueError:
            pass
    print("Master Server Running")
    MasterServer('',port_num).listen()
