#!/usr/bin/env python

import sys
import os
import logging
logging.basicConfig(level=logging.DEBUG)

sys.path.append('gen-py')

# Thrift specific imports
from thrift import Thrift
from thrift.transport import TSocket
from thrift.server import TServer
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

# Protocol specific imports
from metadataServer import MetadataServerService
from blockServer import BlockServerService
from shared.ttypes import *

class MetadataServerHandler():

    def __init__(self, config_path, my_id):
        # Initialize block
        self.confg_path = config_path
        self.port = self.readServerPort(my_id)
        self.files = {}
        self.config_file = config_path
        #self.blockPort = self.getBlockServerPortMeta(config_path)
        #self.blockSock = self.getBlockServerSocketPortMeta(self.blockPort)

    def getFile(self, filename):
        # Function to handle download request from file
        # Return file
        print "Searching in local data structure"
        if filename in self.files:
            f = self.files[filename]
            f.status = responseType.OK
            return f
        else:
            print "File name: ",filename ," not found, returning a null file with ERROR as status"
            f = file()
            f.status = responseType.ERROR
            return f

    def storeFile(self, file):
        # Function to handle upload request
        # Return uploadResponse
        u = uploadResponse()
        u.status = uploadResponseType.OK
        newHashList = []
        blockPort = self.getBlockServerPortMeta(self.config_file)
        blockSock = self.getBlockServerSocketPortMeta(blockPort)

        # Check if each hash in hashList is in Block Server
        for hash in file.hashList:
            hbCheck = blockSock.getBlock(hash)
            # Hash is not in block server
            if hbCheck.status == 'ERROR':
                 newHashList.append(hash)
                 print "Appended ", hash, " to newHashList"
                 print "Filename: ", file.filename
                 u.hashList = newHashList
                 u.status = uploadResponseType.MISSING_BLOCKS
            # Hash is in the block server
            else:
                 print "Already in the block server"
                 newHashList.append(hash)
                 u.hashList = newHashList

        # If status is still OK, then add file to the Metadata Server
        if u.status == uploadResponseType.OK:
            print "Storing file with filename: ", file.filename
            self.files[file.filename] = file

        return u


    def deleteFile(self, f):
        # Function to handle download request from file
        # Return response
        if f.filename not in self.files:
            print "Given hash for deletion not present locally"
            r = response()
            r.message = responseType.ERROR
            return r
        else:
            print "Deleting block"
            del self.files[f.filename]
            r = response()
            r.message = responseType.OK
            return r

    def readServerPort(self, my_id):
        # Get the server port from the config file.
        # id field will determine which metadata server it is 1, 2 or n
        # Your details will be then either metadata1, metadata2 ... metadatan
        # return the port

        print "Checking validity of the config path"
        if not os.path.exists(config_path):
            print "ERROR: Config path is invalid"
            exit(1)
        if not os.path.isfile(config_path):
            print "ERROR: Config path is not a file"
            exit(1)

        metadataID = 'metadata'
        metadataID += str(my_id)
        print "Reading config file"
        with open(config_path, 'r') as conffile:
            lines = conffile.readlines()
            for line in lines:
                if metadataID in line:
                    # Important to make port as an integer
                    return int(line.split()[1].lstrip().rstrip())

        # Exit if you did not get metadataserver information
        print "ERROR: metadata server information not found in config file"
        exit(1)

    # Add other member functions if needed
    def getBlockServerPortMeta(self, config_path):
        # This function reads config file and gets the port for block server
    
        print "Checking validity of the config path"
        if not os.path.exists(config_path):
            print "ERROR: Config path is invalid"
            exit(1)
        if not os.path.isfile(config_path):
            print "ERROR: Config path is not a file"
            exit(1)
    
        print "Reading config file"
        with open(config_path, 'r') as conffile:
            lines = conffile.readlines()
            for line in lines:
                if 'block' in line:
                    # Important to make port as an integer
                    return int(line.split()[1].lstrip().rstrip())
    
        # Exit if you did not get blockserver information
        print "ERROR: blockserver information not found in config file"
        exit(1)
    
    
    def getBlockServerSocketPortMeta(self, port):
        # This function creates a socket to block server and returns it
    
        # Make socket
        transport = TSocket.TSocket('localhost', port)
        # Buffering is critical. Raw sockets are very slow
        transport = TTransport.TBufferedTransport(transport)
        # Wrap in a protocol
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        # Create a client to use the protocol encoder
        client = BlockServerService.Client(protocol)
    
        # Connect!
        print "Connecting to block server on port", port
        try:
            transport.open()
        except Exception as e:
            print "ERROR: Exception while connecting to block server, check if server is running on port", port
            print e
            exit(1)
    
        return client

# Add additional classes and functions here if needed


if __name__ == "__main__":

    if len(sys.argv) != 3:
        print "Invocation <executable> <config_file> <id>"
        exit(-1)

    config_path = sys.argv[1]
    my_id = sys.argv[2]

    print "Initializing metadata server"
    handler = MetadataServerHandler(config_path, my_id)
    port = handler.readServerPort(my_id)
    # Define parameters for thrift server
    processor = MetadataServerService.Processor(handler)
    transport = TSocket.TServerSocket(port=port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    # Create a server object
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
    print "Starting server on port : ", port

    try:
        server.serve()
    except (Exception, KeyboardInterrupt) as e:
        print "\nExecption / Keyboard interrupt occured: ", e
        exit(0)
