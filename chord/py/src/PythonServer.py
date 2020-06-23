
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#
import os
import glob
import re
import sys
import hashlib
import socket
sys.path.append('gen-py')
sys.path.insert(0, glob.glob('/home/yaoliu/src_code/local/lib/lib/python2.7/site-packages')[0])

from chord import FileStore
from chord.ttypes import SystemException, RFileMetadata, RFile, NodeID

#from shared.ttypes import SharedStruct

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

def createClient(ip,port):
    transport = TSocket.TSocket(ip,port)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = FileStore.Client(protocol)
    transport.open()
    return transport,client
def sha_256(meta):
    sha256=hashlib.sha256()
    sha256.update(meta)
    k=sha256.hexdigest()

    return k

def sha_256_id(host,port):
    
    meta=host+":"+port
    sha256=hashlib.sha256()
    sha256.update(meta)
    k=sha256.hexdigest()

    return k
def createNode(id1,ip,port):
    node = NodeID()
    node.id=id1
    node.ip = ip
    node.port = port
    return node
class Handler():
    fingerTable=[]
    rfileArray=[]
    currentNode=NodeID()

    files=[]
    host_name = socket.gethostname() 
    host= socket.gethostbyname(host_name)
    port=sys.argv[1]
    id1=sha_256_id(host,port)
    currentNode.id=id1
    currentNode.ip=host  
    currentNode.port=port
    print currentNode
    def writeFile(self, rFile):
        
        exists=False
        node=self.findSucc(rFile.meta.contentHash)
        if node!=self.currentNode:
            raise SystemException("WriteFile, file does not exist")
        for x in self.files:
            if x.get("filename"==rFile.meta.filename):
                exists=True
        if exists:
            rFile.meta.version=rFile.meta.version+1
            file1 = open(rFile.meta.filename,"w")
            file1.write(rFile.content)
        else:
            rFile.meta.version=1
            file1 = open(rFile.meta.filename,"w")
            rFile.contentHash=sha_256(rFile.meta.filename)
            file1.write(rFile.content) 
        self.files.append(rFile)
    def readFile(self, filename):
        key = sha_256(filename)
        node=self.findSucc(key)
        if node!=self.currentNode:
            raise SystemException("ReadFile, file does not exist")
        exists=False
        index=0
        for x in range(0,len(self.files)):
            if self.files[x].meta.filename==filename:
                exists=True
                index= x
        temp=self.files[index]
        rFile1 = RFile()
        if exists:
            print("exist")
            meta = RFileMetadata()
            meta.filename = filename
            meta.version=temp.meta.version
            meta.contentHash=temp.meta.contentHash
            rFile1.meta = meta
            rFile1.content=temp.content
            
            return rFile1
        raise SystemException("ReadFile, file not found")


    def setFingertable(self, node_list):
        for i in node_list:
            node=createNode(i.id,i.ip,i.port)
            self.fingerTable.append(node)
        
    def findSucc(self, key):
        if len(self.fingerTable)==0:
            raise SystemException("no finger table")

        n = self.findPred(key)
        if not n.id == self.currentNode.id:
            transport,client=createClient(n.ip,n.port)
            s = client.getNodeSucc()
            transport.close()
            return s 
        else:
            return self.getNodeSucc()

    def getNodeSucc(self):
        if len(self.fingerTable)==0:
            raise SystemException("no finger table")
        return self.fingerTable[0]
    def findPred(self, key):   
        if len(self.fingerTable)==0:
            raise SystemException("no finger table")
        # ***************** check first finger table index *******************
        if self.currentNode.id >= self.fingerTable[0].id:
            if not (key <= self.currentNode.id and key > self.fingerTable[0].id):
                n=createNode(self.currentNode.id,self.currentNode.ip,int(self.currentNode.port))
                return n
        else:
            if key > self.currentNode.id and key <= self.fingerTable[0].id:
                n=createNode(self.currentNode.id,self.currentNode.ip,int(self.currentNode.port))
                return n
        # ***************** check other finger table index *******************
        if self.currentNode.id >= key:
            for i in range(len(self.fingerTable)-1,0,-1):
                if not (self.fingerTable[i].id <= self.currentNode.id and self.fingerTable[i].id > key):
                    t,c=createClient(self.fingerTable[i].ip,self.fingerTable[i].port)
                    k = c.findPred(key)
                    t.close()
                    return k
            raise SystemException("FindPred error")
        else:
            for i in range(len(self.fingerTable)-1,0,-1):
                if self.fingerTable[i].id > self.currentNode.id and self.fingerTable[i].id <= key:
                    transport,client=createClient(self.fingerTable[i].ip,self.fingerTable[i].port)
                    k = client.findPred(key)
                    transport.close()
                    return k
            raise SystemException("FindPred error")
            

if __name__ == '__main__':

    handler = Handler()
    processor = FileStore.Processor(handler)
    transport = TSocket.TServerSocket(port=int(sys.argv[1]))
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)


    print('Starting the server...')
    server.serve()
    print('done.')
 