#!/usr/bin/env python

import sys
import os
import hashlib

sys.path.append('gen-py')

# Thrift specific imports
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from blockServer import *
from metadataServer import *
from shared.ttypes import *
from metadataServer.ttypes import *
from blockServer.ttypes import *

from os import listdir
from os.path import isfile

# Add classes / functions as required here
def getBlockServerPort(config_path):
    # This function reads config file and gets the port for block server

    #print "Checking validity of the config path"
    if not os.path.exists(config_path):
        print "ERROR"
        exit(1)
    if not os.path.isfile(config_path):
        print "ERROR"
        exit(1)

    #print "Reading config file"
    with open(config_path, 'r') as conffile:
        lines = conffile.readlines()
        for line in lines:
            if 'block' in line:
                # Important to make port as an integer
                return int(line.split()[1].lstrip().rstrip())

    # Exit if you did not get blockserver information
    print "ERROR"
    exit(1)


def getBlockServerSocket(port):
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
    #print "Connecting to block server on port", port
    try:
        transport.open()
    except Exception as e:
        print "ERROR"
        print e
        exit(1)

    return client

def getMetadataServerPort(config_path):
    # This function reads config file and gets the port for block server

    #print "Checking validity of the config path"
    if not os.path.exists(config_path):
        print "ERROR"
        exit(1)
    if not os.path.isfile(config_path):
        print "ERROR"
        exit(1)

    #print "Reading config file"
    with open(config_path, 'r') as conffile:
        lines = conffile.readlines()
        for line in lines:
            if 'metadata' in line:
                # Important to make port as an integer
                return int(line.split()[1].lstrip().rstrip())

    # Exit if you did not get blockserver information
    print "ERROR"
    exit(1)

def getMetadataServerSocket(port):
    # This function creates a socket to block server and returns it

    # Make socket
    transport = TSocket.TSocket('localhost', port)
    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)
    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    # Create a client to use the protocol encoder
    client = MetadataServerService.Client(protocol)

    # Connect!
    #print "Connecting to metadata server on port", port
    try:
        transport.open()
    except Exception as e:
        print "ERROR"
        print e
        exit(1)

    return client

def do_operations(sock, metaSock, base_dir, command, filename):
    # Check what the command is and work accordingly
    # Upload command
    if command == 'upload':
        # Create blockList to hold data for hashes
        blockList = {}
        fsHashList = []

        # Break the filename into 4MB blocks
        blockSize = 4 * 1024 * 1024

        # Check if file exists in the directory
        fileToOpen = base_dir + '/' + filename
        if not os.path.exists(fileToOpen):
            print "ERROR"
            return

        f = open(fileToOpen, 'rb')
        #print "Reading ",fileToOpen, " file"
        data = f.read(blockSize)
        #print "Read ",len(data), " from ",filename, " file"
        #print "Generating SHA-256 hash"

        # Create file struct to put list of hash
        fs = shared.ttypes.file()
        fs.version = 1
        fs.filename = filename
        fs.status = responseType.OK

        # Check if file was empty first before going into loop
        if data == "":
            m = hashlib.sha256()
            m.update(data)
            hashString = m.hexdigest()
            #print "SHA-256 hash for the block : ", hashString

            blockList[hashString] = data
            fsHashList.append(hashString)
            fs.hashList = fsHashList

            # Check if the block is already in block server with storeFile(),
            fileCheck = metaSock.storeFile(fs)

            # Block is already in block server, no missing blocks
            if fileCheck.status == responseType.OK:
                print "OK"
                return

            # Not in server so create mapping
            else:
                #print "Storing the hash on block server via StoreBlock RPC"
                hb = hashBlock()
                hb.hash = hashString
                hb.block = blockList[hashString]
                hb.status = "OK"

                try:
                    resp = sock.storeBlock(hb)
                except Exception as e:
                    print "ERROR"
                    print e
                    exit(1)
                #print "Received response from block server"
            
                fileCheck = metaSock.storeFile(fs)

                if fileCheck.status == responseType.OK:
                    print "OK"
                    return
                else:
                    print "ERROR"
                    return

        # Fill fs.hashList with 4MB blocks
        while data != "":
            m = hashlib.sha256()
            m.update(data)
            hashString = m.hexdigest()
            #print "SHA-256 hash for the block : ", hashString
        
            blockList[hashString] = data
            fsHashList.append(hashString)
            fs.hashList = fsHashList

            data = f.read(blockSize)
            #print "Read ",len(data), " from ",filename, " file"

        # Store the blocklist onto block server then create mapping
        fileCheck = metaSock.storeFile(fs)

        # If all blocks are already on block server
        if fileCheck.status == responseType.OK:
            print "OK"
            return

        # If status is not OK, store file to create mapping
        else:
            for hashString in fileCheck.hashList:

                # Create a hashBlock struct to be stored in block server
                #print "Storing the hash on block server via StoreBlock RPC"
                hb = hashBlock()
                hb.hash = hashString
                hb.block = blockList[hashString]
                hb.status = "OK"

                try:
                    resp = sock.storeBlock(hb)
                except Exception as e:
                    print "ERROR"
                    print e
                    exit(1)
                #print "Received response from block server"

                #if resp.message != responseType.OK:
                #    print "Server said OK, block upload successful"
                #else:
                #    print "Server said ERROR, block upload unsuccessful"

            fileCheck = metaSock.storeFile(fs)

            if fileCheck.status == responseType.OK:
                print "OK"
                return
            else:
                print "ERROR"
                return

    # Download command
    if command == 'download':
        # Create a local list that will hold all 4MB hash blocks of local files
        localList = {}
        blockSize = 4 * 1024 * 1024

        # Scan the base_dir for all files then separate those files into blocks
        fileList = [f for f in listdir(base_dir) if isfile(f)]
        for file in fileList:
            # Split the files into 4MB blocks and put into localList
            f = open(file, 'rb')
            #print "Reading ",file, " file"
            data = f.read(blockSize)
            #print "Read ",len(data), " from ",filename, " file"
            #print "Generating SHA-256 hash"
            
            # Empty file
            if data == "":
                m = hashlib.sha256()
                m.update(data)
                hashString = m.hexdigest()
                #print "SHA-256 hash for the block : ", hashString

                localList[hashString] = data

            # Not empty file
            while data != "":
                m = hashlib.sha256()
                m.update(data)
                hashString = m.hexdigest()
                #print "SHA-256 hash for the block : ", hashString
            
                localList[hashString] = data

                data = f.read(blockSize)
                #print "Read ",len(data), " from ",filename, " file"

        # Get the file for the blocklist through the filename
        fileRecv = metaSock.getFile(filename)

        # If filename not found in metadata server
        if fileRecv.status == responseType.ERROR:
            # Error throw or something
            print "ERROR"
            return

        # Filename is found in metadata server
        else:
            # Combine name of base_dir and text
            completePath = base_dir + '/' + fileRecv.filename

            #print fileRecv.filename, " is the file to be created"
            newFile = open(completePath, "w+")
            # If blocks not in base_dir then download
            for hashString in fileRecv.hashList:
                # Block not in base_dir, must download
                if hashString not in localList:
                    #print "Now trying to retrieving the same hashblock using GetBlock RPC"
                    try:
                        hb_dwnld = sock.getBlock(hashString)
                    except Exception as e:
                        print "ERROR"
                        print e
                        exit(1)
                    
                    #print "Received block, checking status"
                    if hb_dwnld.status == "ERROR":
                        print "ERROR"
                        return

                    #print "Not in localList, writing"
                    newFile.write(hb_dwnld.block)
                    
                # Block is in base_dir, write block to list
                else:
                    #print "Is in localList, writing"
                    newFile.write(localList[hashString])

            newFile.close()
        print "OK"
        return

    # Delete command
    if command == 'delete':

        f = metaSock.getFile(filename)

        if f.status == responseType.ERROR:
            print "ERROR"
            return
        else:
            try:
                resp = metaSock.deleteFile(f)
            except Exception as e:
                print "ERROR"
                print e

            if resp.message == responseType.OK:
                print "OK"
            else:
                print "ERROR"

if __name__ == "__main__":

    if len(sys.argv) != 5:
        print "Invocation : <executable> <config_file> <base_dir> <command> <filename>"
        exit(-1)

    #print "Starting client"

    config_path = sys.argv[1]
    #print "Configuration file path : ", config_path
    #print "Creating socket to Block Server"
    servPort = getBlockServerPort(config_path)
    sock = getBlockServerSocket(servPort)

    metaPort = getMetadataServerPort(config_path)
    metaSock = getMetadataServerSocket(metaPort)

    base_dir = sys.argv[2]
    #print "Base directory : ", base_dir

    command = sys.argv[3]
    #print "Command : ", command

    filename = sys.argv[4]
    #print "Filename : ", filename

    # Time to do some operations!
    do_operations(sock, metaSock, base_dir, command, filename)

    '''
    Server information can be parsed from the config file

    connections can be created as follows

    Eg:

    # Make socket
    transport = TSocket.TSocket('serverip', serverport)

    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)

    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    # Create a client to use the protocol encoder
    client = HelloService.Client(protocol)

    # Connect!
    try:
        transport.open()
    except Exception as e:
        print "Error while opening socket to server\n", e
        exit(1)

    # Create custom data structure object
    m = message()

    # Fill in data
    m.data = "Hello From Client!!!"

    # Call via RPC
    try:
        dataFromServer = client.HelloServiceMethod(m)
    except Exception as e:
        print "Caught an exception while calling RPC"
        # Add handling code
        exit(1)

    '''
