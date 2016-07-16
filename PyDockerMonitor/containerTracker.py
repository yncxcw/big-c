#!/usr/bin/python
import os

import time
import logging
import Pyro4
from hostStatusUpdateRequest import HostUpdate,ContainerUpdate,ContainerAction
from hostStatusUpdateResponse import ContainerCommand,HostResponse,ContainerResponse 
from containerScheduler import ContainerScheduler
from hostToContainerManager import HostToContainerManager
from YarnCommand import YarnCommand

log=logging.getLogger("RMDocker.ContainerTracker")

class ContainerTracker:

    def __init__(self,configure):
        self.hostToTimeStamp  = {}
        self.configure = configure 
        ##keep last current time that receive heartbeat
        self.lastCurrent = 0
        ##containerScheduler to make up commands to each node
        self.containerScheduler=None
        ##keep map from host to containers
        self.hostToContainerManager=None  
        ##initialize configure object, passed down by nameserver


    def initialize(self):
        self.lastCurrent = self.currentTime()
	    ##make sure the configure object is not null
        if self.configure is None:
	        return False
        if self.hostToContainerManager is None:
            self.hostToContainerManager= HostToContainerManager(self.configure)
        
        if self.containerScheduler is None:
            self.containerScheduler = ContainerScheduler(self.hostToContainerManager)
        
        log.info("initialize successfully")
        return True 

    def currentTime(self):
	    return int(round(time.time()*1000))	

    @Pyro4.expose
    def register(self,host):
	    ## we have already seen this host before
        if host in self.hostToTimeStamp.keys():
            ##TODO log error message here
            return False
	    ##TODO record this machine in log
        log.info("register host: %s", host)			
        self.hostToContainerManager.register(host)
        self.containerScheduler.register(host)
        self.hostToTimeStamp[host]=self.currentTime()

        return True

    @Pyro4.expose
    def statusUpdate(self, dict_hostUpdate):
        hostUpdate=HostUpdate._dict_to_class_(dict_hostUpdate)
        #log.info("get heartbeat: %s",hostUpdate.getHost()) 
        host = hostUpdate.getHost()
        if  host not in self.hostToTimeStamp.keys():
            log.error("unknown host %s",host)
            return 
        ##update time stamp for this hos
        self.hostToTimeStamp[host] = self.currentTime() 
        ##if it's a regular heartbeat, we do nothing here
        if len(hostUpdate.getContainerUpdates()) == 0:
            return
        ##iterate all containers in status and do operations
        self.hostToContainerManager.update(hostUpdate)
        ## delete timeout host
        to_delete=[]
        for thost in self.hostToTimeStamp.keys():
            ##we get rid of one host if we can't hear heartbeat for 1000s
            if (self.currentTime() - self.hostToTimeStamp[thost])/1000 > 20*50:
                log.info("delete out of time host %s",thost)
                to_delete.append(thost)

        for thost in to_delete:
            self.hostToContainerManager.deregister(thost)
            self.containerScheduler.deregister(thost)
            del self.hostToTimeStamp[thost]

        ##we notify the scheduler and do information update
        self.containerScheduler.notify(host)
        hostResponse = self.containerScheduler._make_hostResponse_(host)
        dict_hostResponse = HostResponse._class_to_dict_(hostResponse)
        return dict_hostResponse   

    @Pyro4.expose
    def containerCommand(self,dict_containerCommand):
        command = YarnCommand._dict_to_class_(dict_containerCommand)
        if command.get_id() is None:
            return False
        log.info("get command for container %s",command.get_id())
        return self.containerScheduler.schedule(command)

    ##TODO get container run time information
    @Pyro4.expose
    def containerPoll(self,containerId):
        pass

    def serviceLoop(self):
        pass

    def start(self):
        pass            
        

