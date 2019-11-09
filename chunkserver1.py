#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os
from rpyc.utils.server import ThreadedServer
import rpyc
class ChunkServer1(rpyc.Service):
    def __init__(self):
        
        self.local_chunk_table={}
        self.chunkserver_filesystem_loc=os.getcwd()+"/chunkserverdirectory1"
        if not os.access(self.chunkserver_filesystem_loc,os.W_OK):
            os.mkdir(self.chunkserver_filesystem_loc)
    
    def write_to_file(self,chunk_id,filename,chunk):
        chunkserver_filename=self.chunkserver_filesystem_loc+"/"+str(filename)+str(chunk_id)
        with open(chunkserver_filename,"a+") as f:
            f.write(chunk)
        self.local_chunk_table[filename,chunk_id]=chunkserver_filename
        print(self.local_chunk_table)
    
    def exposed_echo(self):
        print("Hello from ChunkServer1")


# In[ ]:


if __name__ == "__main__":
    a=ChunkServer1()
    a.write_to_file(2,'b.txt','Hi this is Swapnil')
    t = ThreadedServer(ChunkServer1, port=5678)
    t.start()

