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
    list1=[5067,5068]
    for i in list1:
        s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        s.connect((socket.gethostbyname('localhost'),i))
        s.send(b"Hi from Client")
    







if __name__=="__main__":
    chunks=connect_to_master_server()
    connect_to_chunk_server(chunks)



