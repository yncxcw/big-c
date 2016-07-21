#!/usr/bin/python

import logging
from hostStatusUpdateRequest import HostUpdate,ContainerUpdate,ContainerAction
from netFlowAnalyze import NetflowAnalyze

log=logging.getLogger("RMDocker.HostToContainerManager")

class HostToContainerManager:


    def __init__(self,configure):
        self.configure        = configure
        self.netAnalyze       = NetflowAnalyze()
        self.hostToContainers = {}
        ##keep live containers name
        self.liveContainers   = {}  

    ##called when the host send first hearbeat
    def register(self,host):
        self.hostToContainers[host] = None

    ##called when the host is out of date
    def deregister(self,host):
        log.info("deregister host %s from hostToContainerManager", host)
        del self.hostToContainers[host]

    def getContainerByName(self,containerName):
        log.info("try to get container by name")
        if self.liveContainers.keys is None:
            log.info("live container is none")
            return None
        elif containerName not in self.liveContainers.keys():
            log.info("container is not in live container")
            return None
        else:
            return self.liveContainers[containerName]


    def update(self,hostUpdate):
        host = hostUpdate.getHost()
        #log.info("get host update from %s",host)
        for status in  hostUpdate.getContainerUpdates():
            ##new container found on host
            if status.getAction() == ContainerAction.NEW:
                container = CTContainer(id=status.getID(),name=status.getName(),host=host)
                log.info("container %s from %s update and action is NEW",status.getName(),host)
                ##initialize with configure file
                container.initialize(self.configure)
                ##update/initialize the configure file
                container.updateCgroup(status.getCgroupKeyValues())
                ##add to name to container mapping
                self.liveContainers[status.getName()] = container
                ## this is first container on this host
                if self.hostToContainers[host] == None:
                    self.hostToContainers[host] = []
                    self.hostToContainers[host].append(container)
                else:
                    self.hostToContainers[host].append(container)
            ##delete this container from host 
            elif status.getAction() == ContainerAction.DIE:
                log.info("container %s from %s update and action is DELETE",status.getName(),host)
                container = self.findContainerOnHost(host,status.getID()) 
                if container == None:
                    log.error("container %s not found when deleting",status.getName())
                else:
                    self.hostToContainers[host].remove(container) 
                    del self.liveContainers[status.getName()]
            elif status.getAction() == ContainerAction.UPDATE:
                log.info("container %s from %s update and action is UPDATE",status.getName(),host)
                container = self.findContainerOnHost(host,status.getID()) 
                if container == None:
                    log.error("container %s not found when updating",status.getName())
                else:
                    ##analyzie network
                    self.netAnalyze.update(container.getName(),status.getNetflow())
                    container.updateCgroup(status.getCgroupKeyValues())

         


    def findContainerOnHost(self,host,id):
        if host not in self.hostToContainers.keys():
            log.error("container %s found error",id)
            return None
        for container in self.hostToContainers[host]:
            if id == container.getID():
                return container
        return None

    def findContainerOnHostByName(self,host,name):
        if host not in self.hostToContainers.keys():
            log.error("container %s found error",name)
            return None
        for container in self.hostToContainers[host]:
            if id == container.getName():
                return container
        return None

    def getContainersOnHost(self,host):
        if host not in self.hostToContainers.keys():
            log.error("host name eror")
            return None
        return self.hostToContainers[host]



class CTContainerStatus:
    RUN    = 0
    SUSPEND= 1
    BOOST  = 3

class CTContainer:

    def __init__(self,id,name,host):
        self.name = name
        self.host = host
        self.id   = id
        self.cgroups={}
        self.status = CTContainerStatus.RUN
        self.defaultCgroupKeyValue={}
    
    ##initialize one container by configure structure 
    def initialize(self,configure):
        cgroupNames = configure.get("cgroup")
        for name in cgroupNames: 
            cgroup = CTCGroup(name)
            cgroup.initialize(configure)
            self.cgroups[name] = cgroup


    def put(self,name,key,value):
        if self.defaultCgroupKeyValue.get(name) is None:
            self.defaultCgroupKeyValue[name]={}

        try:        
            self.defaultCgroupKeyValue[name][key]=value
        except Exception as error:
            log.error("key value error when try to put key value %s",error) 


    def get(self,name,key):
        try:
            return self.defaultCgroupKeyValue[name][key]
        except Exception as error:
            log.error("key value error when try to get default key value %s",error)
            return None

    def setStatus(self,status):
        self.status=status

    def getStatus(self):
        return self.status

    def getName(self):
        return self.name

    def getID(self):
        return self.id

    def getHost(self):
        return self.host

    def getCgroups(self):
        return self.cgroups.values()

    def getCgroup(self,name):
        if name not in self.cgroups.keys():
            return None
        return self.cgroups[name]

    def getCgroupValue(self,name,key):
        if name not in self.cgroups.keys():
            log.info("update name is not in configure")
            return None
        return self.cgroups[name].getValue(key)

    def updateCgroup(self,cgroupKeyValues):
        for cgroup in cgroupKeyValues.keys():
            self.updateCgroupKeyValues(cgroup,cgroupKeyValues[cgroup]) 

    def updateCgroupKeyValues(self,name,keyValues):
        if name not in self.cgroups.keys():
            log.info("update name is no in configure")
            return
        for key,value in keyValues.items():
            ##TODO for debug
            log.info("container %s get key %s, value %s",self.getName(),key,value)
            self.cgroups[name].updateValue(key,value)

    def updateCgroupKeyValue(self,name,key,value):
        self.cgroups[name].updateValue(key,value)

class CTCGroup:

    def __init__(self,name):
        self.name = name
        self.parameters={}

    def initialize(self,configure):
        ##register for parameters under cgroup
        parameters = configure.get(self.name)
        if type(parameters) is str:
            self.parameters[parameters] = None
        elif type(parameters) is list:
            for parameter in parameters:
                self.parameters[parameter]=None
        else:
            pass
            log.error("parameters initialize error in CTCGroup")
        
    def updateValue(self,key,value):
        if key in self.parameters.keys():
            self.parameters[key] = value
        else:
            pass

    def getValue(self,key):
        if key in self.parameters.keys():
            return self.parameters[key]
        else:
            return None




