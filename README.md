# GFS_19_ARRAY

Implementation of Google File System in Python 3


Testing Formats:
python3 Master_Server.py: To run the Master Server<br>
python3 Backup_Master_Server.py : To run the BackUp Master Server<br>
python3 chunk_server.py port_number(of chunkserver):To run the chunkserver [All the chunkservers to be run with their port numbers]<br>
python3 client.py: To run the client<br>

In client after running it:<br>
upload file_name: To upload the file into the chunkservers<br>
download file_name: To download the file from the chunkservers<br>
lease file_name: Put a lease/lock on the file,so that no other client can upload/download the file<br>
unlease file_name: Remove the lease put on the file...<br>





Architecture:<br>
One Master Server, One BackUp Master Server,Four Chunkservers,Multiple Clients Allowed.<br>
Master Server will hold the Metadata of all the chunks of the files which will be used by clients and chunkservers to communicate with the appropriate chunkserver.<br>



