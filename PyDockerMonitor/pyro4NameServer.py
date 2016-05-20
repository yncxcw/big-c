#!/usr/bin/python

import os

import __init__
import logging

import Pyro4
from containerTracker import ContainerTracker
from configure import Configure


__init__._check_every_thing_("master")


log = logging.getLogger("RMDocker.Pyro4NameServer")

class Pyro4NameServer:

    ##default host name for NameServer	
    host = "localhost"

    ##default host port for NameServer
    port = 51681

    ##default object id for NameServer
    ID = "pyro4NameServer"	 

    ##daemon of NameServer
    daemon = None
	
    ##initialize configure
    configure = Configure()

    ##containerTacker, main service implimentation,  communicate with slaves
    ##through heartbeat
    containerTracker = None

    def initialize(self):
	    ##initialize configure object
        if self.configure.initialize() is False:
            log.error("configure initialize failure")
            return False
	    ##initialize host name, port and object ID
        self.host = self.configure.get("nameserverHost")
        if self.host is None:
            log.error("host is missing for configure")
            return False
        self.port = int(self.configure.get("daemonserverPort"))
        if self.port is None:
            log.error("port is missing for configure")
            return False
        self.ID = self.configure.get("nameserverID")
        if self.ID is None:
            log.error("nameserver ID is missing for configure")
            return False
        self.containerTracker = ContainerTracker(self.configure)
        if self.containerTracker.initialize() is False:
            log.error("containerTracker initialize failed")
            return False

        log.info("name server initialize successfully")
        return True

    def start(self):
    	##start Name Server service
        try:
            self.daemon = Pyro4.Daemon(host=self.host,port = self.port)
            nameServer  = Pyro4.locateNS()
            nameURI     = self.daemon.register(self.containerTracker)
            nameServer.register(self.ID,nameURI)
            log.info("start pyro4 Name Server") 
            self.daemon.requestLoop()
        except Exception as error:
            log.error("error when set up server %s",error)
        ##start containerTracker main loop
        self.containerTracker.start()


    def serviceLoop(self):
        if self.initialize() is False:
            log.info("serviceLoop failed due to initialize fail")
            return
        self.start()

    def stop(self):
        pass	
	

if __name__=="__main__":

    pyro4NameServer = Pyro4NameServer()
    pyro4NameServer.serviceLoop()
				 	
			
	 
