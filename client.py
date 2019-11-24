import socket
import os
import pickle

#Connecting to the master_server
def connect_to_master_server(getCommand):
    s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    s.connect((socket.gethostbyname('localhost'),7082))

    decision, filename=getCommand.split(' ')
    # print(decision)
    # print(filename)
    if(decision=="upload"):
        size=str(os.path.getsize(filename))
        fileplussize="client"+":upload:"+filename+":"+size
        s.send(bytes(fileplussize,"utf-8"))
        chunks=[]
        getz=s.recv(2048).decode('utf-8')
        getz=int(getz)+1000
        # getz=int(getz)
        # print(getz)
        chunks=pickle.loads(s.recv(getz))
        return chunks
    if(decision=="download"):
        f_download="client"+":download:"+filename+":dummydata"
        s.send(bytes(f_download,"utf-8"))
        chunks=[]
        getz1=s.recv(2048).decode('utf-8')
        getz1=int(getz1)+1000
        # print(getz)
        chunks=pickle.loads(s.recv(getz1))
        return chunks



#Connecting to the chunk_server
def connect_to_chunk_server(decision,chunks,filename):
    list1=[6467,6468,6469,6470]
    
    if(decision=="upload"):
        chunks_list=[]
        f=open(filename,'rb')
        data=f.read(2048)

        while data:
            chunks_list.append(data)
            data=f.read(2048)
        
        #Sending chunks to the appropriate chunkserver
        for chunk_id,chunk_server in chunks:
            s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            s.connect((socket.gethostbyname('localhost'),list1[chunk_server-1]))
            # filename="file2.txt"
            to_send="client:"+"upload:"+str(chunk_server)+":"+str(chunk_id)+":"+filename+":"
            #print(to_send)
            to_send=to_send.ljust(400,'~')
            # print(len(to_send.encode('utf-8')))
            #print(to_send)
            s.send(str(to_send).encode("utf-8"))
            s.send(chunks_list[chunk_id-1])

    data=""
    if(decision=="download"):

            for chunk_id,chunk_server in chunks:
                s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                s.connect((socket.gethostbyname('localhost'),list1[chunk_server-1]))
                to_send="client:"+"download:"+str(chunk_server)+":"+str(chunk_id)+":"+filename+":"    
                to_send=to_send.ljust(400,'~')
                s.send(str(to_send).encode("utf-8"))
                
                filesystem = os.getcwd()+"/Client"

                if not os.access(filesystem, os.W_OK):
                    os.makedirs(filesystem)

                filesystem=filesystem+"/"+str(filename)            
                with open(filesystem, 'ab') as f:

                    c_recv=s.recv(2048)
                    # print(c_recv)
                # data += (c_recv)
                    f.write(c_recv)
                # data += (c_recv.decode("utf-8"))
                    # f.write(c_recv.decode("utf-8"))
            
            
                # f.write(data)    

if __name__=="__main__":

    while True:
        getCommand=input()
        # print(getCommand)
        decision, filename=getCommand.split(' ')
        if(decision=="upload"):
            chunks=connect_to_master_server(getCommand)
            # print(chunks)
            connect_to_chunk_server(decision,chunks,filename)
            print("Uploading of file done!!!")
        if(decision=="download"):
            chunks=connect_to_master_server(getCommand)
            # print(chunks)
            connect_to_chunk_server(decision,chunks,filename)
            print("Downloading of file done!!!")



