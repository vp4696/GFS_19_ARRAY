import socket
import os
import pickle

#Connecting to the master_server
def connect_to_master_server():
    s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    s.connect((socket.gethostbyname('localhost'),7082))
    filename="file1.txt"
    size=str(os.path.getsize(filename))
    
    fileplussize="client"+":"+filename+":"+size
    s.send(bytes(fileplussize,"utf-8"))
    chunks=[]
    chunks=pickle.loads(s.recv(2048))
    return chunks


#Connecting to the chunk_server
def connect_to_chunk_server(chunks):
    list1=[6467,6468,6469,6470]
    
    chunks_list=[]
    f=open("file1.txt",'rb')
    data=f.read(2048)
    #size=os.path.getsize("six.mp3")


    while data:
        chunks_list.append(data)
        data=f.read(2048)
    
    #Sending chunks to the appropriate chunkserver
    for chunk_id,chunk_server in chunks:
        s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        s.connect((socket.gethostbyname('localhost'),list1[chunk_server-1]))
        filename="file1.txt"
        to_send="client:"+str(chunk_server)+":"+str(chunk_id)+":"+filename+":"
        #print(to_send)
        to_send=to_send.ljust(400,'~')
        # print(len(to_send.encode('utf-8')))
        #print(to_send)
        s.send(str(to_send).encode("utf-8"))
        s.send(chunks_list[chunk_id-1])
                

if __name__=="__main__":
    chunks=connect_to_master_server()
    connect_to_chunk_server(chunks)



