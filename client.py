import socket
import os
import pickle


def connect_to_master_server():
    s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    s.connect((socket.gethostbyname('localhost'),7082))
    filename="b.txt"
    size=str(os.path.getsize(filename))
    fileplussize=filename+":"+size
    s.send(bytes(fileplussize,"utf-8"))
    chunks=[]
    chunks=pickle.loads(s.recv(1024))
    print(chunks)
    return chunks

def connect_to_chunk_server(chunks):
    list1=[6467,6468,6469,6470]
    '''
        for i in list1:
        s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        s.connect((socket.gethostbyname('localhost'),i))
        s.send(b"Hi from Client")
        '''
    chunks_list=[]
    f=open("a.txt",'rb')
    data=f.read(200)
    #size=os.path.getsize("six.mp3")
    print(len(chunks))


    while data:
        chunks_list.append(data)
        data=f.read(200)
    
    for chunk_id,chunk_server in chunks:
        s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        s.connect((socket.gethostbyname('localhost'),list1[chunk_server-1]))
        to_send=str(chunk_server)+":"+str(chunk_id)
        s.send(str(to_send).encode("utf-8"))
        s.send(chunks_list[chunk_id-1])
        


        
    







if __name__=="__main__":
    chunks=connect_to_master_server()
    connect_to_chunk_server(chunks)



