#!/usr/bin/python

import logging

import json
from containerFlow import ContainerConnect 

log = logging.getLogger("RMDocker.HostUpdate")

class ContainerAction:

    NONE   = 0
    NEW    = 1
    DIE    = 2
    UPDATE = 3


class HostUpdate:

    def __init__(self, host, containerUpdates=None):
        ##it may indicate that the host is still alive
        self.host = host
        ##container list stores all containes info, it may be 
        ##null if a container action needs to inform tracker   
        self.containerUpdates=containerUpdates
 
    def getHost(self):
        return self.host

    def getContainerUpdates(self):
        return self.containerUpdates   
    
    @staticmethod
    def _class_to_dict_(obj):
        assert(isinstance(obj,HostUpdate))
        dict_containerUpdates=[]
        for containerUpdate in obj.containerUpdates:
           dict_containerUpdates.append(ContainerUpdate._class_to_dict_(containerUpdate)) 
        return{"__name__"        :HostUpdate.__name__,
               "host"            :obj.host,
               "containerUpdates":dict_containerUpdates
              }
    
    @staticmethod
    def _dict_to_class_(dic):
        assert(dic["__name__"]==HostUpdate.__name__)
        containerUpdates=[]
        for dict_containerUpdate in dic["containerUpdates"]:
            containerUpdates.append(ContainerUpdate._dict_to_class_(dict_containerUpdate))
        hostUpdate=HostUpdate(dic["host"],containerUpdates)
        return hostUpdate


class ContainerUpdate:


    ##we may extend this class in the future
    ##so we only keep key parts in construction
    ##function 
    def __init__(self,name,id,action,cgroupKeyValues=None,netflows=None):
        self.name   = name
        self.id     = id
        self.action = action
        self.cgroupKeyValues = cgroupKeyValues
        self.netflows = netflows

    def getName(self):
        return self.name

    def getID(self):
        return self.id

    def getNetflow(self):
        return self.netflows

    def getAction(self):
        return self.action

    def getCgroupKeyValues(self):
        return self.cgroupKeyValues


    @staticmethod
    def _class_to_dict_(obj):
        assert(isinstance(obj,ContainerUpdate))
        netflows = []
        if obj.getNetflow() is not None:
            for net in obj.getNetflow():
                netflows.append(ContainerConnect._class_to_dict_(net))
        else:
            pass
        return{"__name__"       : ContainerUpdate.__name__,
              "name"            : obj.name,
              "id"              : obj.id,
              "action"          : obj.action,
              "cgroupKeyValues" : obj.cgroupKeyValues,
              "netflows"        : netflows      
              }

    @staticmethod
    def _dict_to_class_(dic):
        assert(dic["__name__"]==ContainerUpdate.__name__)
        netflows=[]
        for net in dic["netflows"]:
            netflows.append(ContainerConnect._dict_to_class_(net))
        containerUpdate = ContainerUpdate(name  =dic["name"],
                                          id    =dic["id"]  ,
                                          action=dic["action"],
                                          cgroupKeyValues=dic["cgroupKeyValues"],
                                          netflows = netflows
                                         )
        return containerUpdate 
