#!/usr/bin/python

import os

#os.environ["PYRO_LOGFILE"]="rmdocker.log"
#os.environ["PYRO_LOGLEVEL"]="DEBUG"



import Pyro4
import socket
import time
import json
import logging


import __init__ 
from configure import Configure
from liveContainerManager import LiveContainerManager
from hostStatusUpdateRequest import ContainerAction, HostUpdate,ContainerUpdate 
from hostStatusUpdateResponse import ContainerCommand,HostResponse,ContainerResponse 


__init__._check_every_thing_("slave") 


log=logging.getLogger("RMDocker.ContainerManager")

class ContainerManager:

    
    def __init__(self):
        self.configure = Configure()

        liveContainers = {}

        self.cgroups={}

        self.host = None

        self.isRunning = False

        self.lastCurrent  = 0

        ##manager live contianers and update parameters
        self.liveContainerManager = None
        
        ##default heatbeat interval(s)
        self.heartbeatInterval = 5
 
        ##remote name server proxy  
        self.remoteNameServer = None

        ##remote name server host address
        self.remoteNameServerHost = ""

        ##remote name server host port
        self.remoteNameServerPort = 0000

        ##remote name server object ID
        self.remoteNameServerID = "container.master"


    def initialize(self):
        
        ##initialize current time
        self.lastCurrent = self.currentTime()

        ##initialize host name
        self.host = socket.gethostname()
	
        if self.configure.initialize() is False:
	        log.error("initialize configure error")
	        return False

        if self.configure.get("heartbeatInterval") is not None:
            self.heartbeatInterval = int(self.configure.get("heartbeatInterval"))

        ##These configurations are strictly required
        self.remoteNameServerID  = self.configure.get("nameserverID")
        if self.remoteNameServerID is None:
	        log.error("initialize remote name server error")
	        return False

        self.remoteNameServerHost= self.configure.get("nameserverHost") 
        if self.remoteNameServerHost is None:
            log.error("initialize remote host error")
            return False

        self.remoteNameServerPort= self.configure.get("nameserverPort")
        if self.remoteNameServerPort is None:
            log.error("initialize remote port error")
            return False

        if self.liveContainerManager is None:
            self.liveContainerManager = LiveContainerManager(self.configure,self.host)

        uri = "PYRONAME:"+self.remoteNameServerID+"@"+self.remoteNameServerHost+":"+self.remoteNameServerPort
        log.info("uri: %s",uri)
        try:		
            self.remoteNameServer = Pyro4.Proxy(uri)
        except Exception as error:
            log.error("error when try to connect proxy %s",error)
            return False
        return True		 
		
    def register(self):
        try:
            isSuccess = self.remoteNameServer.register(self.host)
        except Exception as error:
            log.error("error while register %s",error)
            return False
        log.info("register without exception")
        if isSuccess:
            self.isRunning = True
            log.info("register successfully")
            return True
        else:
            log.info("register failed")
            return False

    def statusUpdate(self, hostUpdate):
        dict_hostUpdate=HostUpdate._class_to_dict_(hostUpdate)
        dict_hostResponse=None
        try: 
            dict_hostResponse = self.remoteNameServer.statusUpdate(dict_hostUpdate)
        except Exception as error:
            log.error("exception at status Update:",error)
            pass
        if dict_hostResponse is None:
            return None
        hostResponse = HostResponse._dict_to_class_(dict_hostResponse)
        log.info("response from host: %s",hostResponse.getHost())
        if hostResponse.getHost() != self.host:
            log.error("wrong host during update")
            return None 
        return hostResponse 
                     
    
    def currentTime(self):
        return int(round(time.time()*1000))

    ##main loop of container manager that hearbeat woth master, execute
    ##command from master
    def serviceLoop(self):
        ##initilaze required data structure   
        log.info("initializing") 
        if self.initialize() is False:
            return
        log.info("registerring")
        ##inform master and register itself
        if self.register() is False:
            return     
        log.info("looping")
        while True:
            if self.isRunning == False:
                log.error("host is not running")
                break
            if (self.currentTime() - self.lastCurrent) / 1000 < self.heartbeatInterval:
            ##We do not want to burn cpu cycles so we sleep here
                time.sleep(self.heartbeatInterval/2)
                continue
            self.lastCurrent = self.currentTime()
            ##update live and dead container
            hostUpdate = self.liveContainerManager.updateLiveContainers()
            ##send heartbeat and process respond
            hostResponse=self.statusUpdate(hostUpdate)
            self.liveContainerManager.liveContainerProcess(hostResponse)

        log.info("break from loop")
            
                   

if __name__=="__main__":

    print ("start contaienrManager")
    containerManager = ContainerManager()
    containerManager.serviceLoop()
