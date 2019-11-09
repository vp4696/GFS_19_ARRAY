#!/usr/bin/env python
# coding: utf-8

# In[9]:


import rpyc
def main1():
#         try:
            conn = rpyc.connect("localhost", 2140)
            conn.root.echo("a.txt")
            print("Master Server connected successfully")
 #       except:
  #          print("Unable to connect to master")
if __name__ == "__main__":
    main1()

