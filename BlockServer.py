#!/usr/bin/env python

import sys
import os
import logging
logging.basicConfig(level=logging.DEBUG)

sys.path.append('gen-py')

from blockServer import BlockServerService
from blockServer.ttypes import *
from shared.ttypes import *

from thrift import Thrift
from thrift.transport import TSocket
from thrift.server import TServer
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

class BlockServerHandler():

    def __init__(self, configpath):
        # Initialize using config file, intitalize state etc
        self.confg_path = configpath
        self.port = self.readServerPort()
        self.hashBlocks = {}

    def storeBlock(self, hashBlock):
        # Store hash block, called by client during upload
        print "About to store hashBlock"
        self.hashBlocks[hashBlock.hash] = hashBlock.block
        print "Generating response"
        r = response()
        r.message = responseType.OK
        return r

    def getBlock(self, hash):
        # Retrieve block using hash, called by client during download
        print "Searching in local data structure"
        if hash in self.hashBlocks:
            print "Hash block found"
            hb = hashBlock()
            hb.hash = hash
            hb.block = self.hashBlocks[hash]
            hb.status = 'OK'
            return hb
        else:
            print "Hash block not found, returning a null block with ERROR as status"
            hb = hashBlock()
            hb.hash = hash
            hb.status = 'ERROR'
            return hb

    def deleteBlock(self, hash):
        # Delete the particular hash : block pair
        if hash not in self.hashBlocks:
            print "Given hash for deletion not present locally"
            r = response()
            r.message = responseType.ERROR
            return r
        else:
            print "Deleting block"
            del self.hashBlocks[hash]
            r = response()
            r.message = responseType.OK
            return r

    def readServerPort(self):
        # In this function read the configuration file and get the port number for the server
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

    # Add your functions here

# Add additional classes and functions here if needed


if __name__ == "__main__":

    if len(sys.argv) != 2:
        print "Invocation <executable> <config_file>"
        exit(-1)

    config_path = sys.argv[1]

    print "Initializing block server"
    handler = BlockServerHandler(config_path)

    # Retrieve the port number from the config file so that you could strt the server
    port = handler.readServerPort()

    # Define parameters for thrift server
    processor = BlockServerService.Processor(handler)
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
