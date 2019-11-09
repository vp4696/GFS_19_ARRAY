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
    
    def exposed_write(self):
        if self.file_name in self.file_map:
            pass
        self.file_map[self.file_name] = []
        b = os.path.getsize(self.file_name)
        self.size = b
        #num_chunks =self.numChunks(self.size)
        chunks = self.exposed_allocChunks()
        return chunks
    
    def exposed_numChunks(self,size):
        return int(math.ceil(size/self.chunksize))

    def exposed_allocChunks(self):
        i=0
        chunks=[]
        num_chunks = self.exposed_numChunks(self.size)
        for j in range(0,num_chunks):
            self.file_map[self.file_name].append((j,i))
            chunks.append((j,i))
            i=(i+1)% self.num_chunk_servers
        print(self.file_map)
        return chunks


# In[ ]:


'''
Specifies the file that needs to be uploaded
'''
def  f_name(fn):
    Master_Server.file_name = fn
    name = Master_Server()
    name.exposed_write()


# In[ ]:


'''
Main function-Launches the threaded master server
'''
if __name__ == "__main__":
    read_config()
    f_name('a.txt')
    f_name('b.txt')
    print("Master Server running")
    t = ThreadedServer(Master_Server, port=2132)
    t.start()

