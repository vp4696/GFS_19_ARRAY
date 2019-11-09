#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import configparser
import math
import os
from rpyc.utils.server import ThreadedServer
import rpyc
'''
This function reads the configurable values from the configuration file.
chunksize - Indicates the chunk size of each chunk in the google file system - 2Kb here
chunk_servers - Contains the IP and Port details of the chunk servers
gfs.conf - Contains the configuration information that is required by the master server
'''
def read_config():
    config = configparser.ConfigParser()
    config.read_file(open('gfs.conf'))
    Master_Server.chunksize = (int(config.get('master_server', 'chunk_size')))
    chunk_servers = config.get('master_server','chunk_servers').split(',')
    i=1
    for j in chunk_servers:
        host,port = j.split(':')
        Master_Server.chunk_servers[i]=(host,port)
        i+=1


# In[ ]:


'''
Master Server Class - Implements the functionality of the master server
Functionality - Updates the metadata whenever a new file is uploaded and stores it in a dictionary format with 
                file name as key and containing the tuple (chunk-id,chunkserver containing the corresponding chunk)
                as its value.
write - calculates the file_size and returns the allocated chunks
num_chunks - calculates the number of chunks the file needs to be split into
allocChunks - updates the metadata about the files and returns allocated chunks to write.
'''
class Master_Server(rpyc.Service):
    chunksize=0
    chunk_servers = {}
    file_map = {}
    size = 0
    num_chunk_servers = 4
    file_name = ''
    
    def write(self):
        if self.file_name in self.file_map:
            pass
        self.file_map[self.file_name] = []
        b = os.path.getsize(self.file_name)
        self.size = b
        #num_chunks =self.numChunks(self.size)
        chunks = self.allocChunks()
        return chunks
    
    def numChunks(self,size):
        return int(math.ceil(size/self.chunksize))

    def allocChunks(self):
        i=0
        chunks=[]
        num_chunks = self.numChunks(self.size)
        for j in range(0,num_chunks):
            self.file_map[self.file_name].append((j+1,i+1))
            chunks.append((j+1,i+1))
            i=(i+1)% self.num_chunk_servers
        print(self.file_map)
        return chunks
    
    def connections(self):
        conn1 = rpyc.connect("localhost", int(self.chunk_servers[1][1]))
        conn1.root.echo()
        conn2 = rpyc.connect("localhost", int(self.chunk_servers[2][1]))
        conn3 = rpyc.connect("localhost", int(self.chunk_servers[3][1]))
        conn4 = rpyc.connect("localhost", int(self.chunk_servers[4][1]))
        
        
        
    
    def upload(self,file):
        chunks = self.write()
        self.connections()
        print(chunks)
        for i in range(0,len(chunks)):
            k,l=chunks[i]
            #print(k)
            #print(l)
        
    def exposed_echo(self,file):
        self.file_name = file
        #print(self.file_name)
        self.upload(self.file_name)
    


# In[ ]:


'''
Specifies the file that needs to be uploaded
'''
def  f_name(fn):
    Master_Server.file_name = fn
    name = Master_Server()
    name.write()


# In[ ]:


'''
Main function-Launches the threaded master server
'''
if __name__ == "__main__":
    read_config()
    print(Master_Server.chunk_servers)
#     f_name('a.txt')
#     f_name('b.txt')
    print("Master Server running")
    t = ThreadedServer(Master_Server, port=2140)
    t.start()

