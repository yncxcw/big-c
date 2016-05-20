#!/usr/bin/python

import json


class ContainerCommand:

    ##Node
    NONE                    = 0

    ##update parameter for container
    UPDATE_CGROUP_PARAMETER = 1

    ##kill container
    KILL_CONTAINER          = 2

    ##start container
    START_CONTAINER         = 3



class HostResponse:

    def __init__(self,host,containerResponses = None):
        ##to verify the response is forwared to correct host
        self.host = host
        self.containerResponses = containerResponses

    def getHost(self):
        return self.host

    def getContainerResponses(self):
        return self.containerResponses

    @staticmethod
    def _class_to_dict_(obj):
        assert(isinstance(obj,HostResponse))
        dict_containerResponses = []
        for containerResponse in obj.containerResponses:
            dict_containerResponses.append(containerResponse._class_to_dict_(containerResponse))

        return {"__name__"          :   HostResponse.__name__,
                "host"              :   obj.host             ,
                "containerResponses":   dict_containerResponses
                }

    @staticmethod
    def _dict_to_class_(dic):
        assert(dic["__name__"] == HostResponse.__name__)
        containerResponses = []
        for dict_containerResponse in dic["containerResponses"]:
            containerResponses.append(ContainerResponse._dict_to_class_(dict_containerResponse))
        hostResponse = HostResponse(dic["host"],containerResponses)
        return hostResponse
        


class ContainerResponse:

    def __init__(self,id,command,cgroupKeyValues=None):
        self.id = id
        self.command = command
        self.cgroupKeyValues = cgroupKeyValues

    def getID(self):
        return self.id

    def getCommand(self):
        return self.command

    def getCgroupKeyValues(self):
        return self.cgroupKeyValues

    @staticmethod
    def _class_to_dict_(obj):
        assert(isinstance(obj,ContainerResponse))
        return{"__name__"       : ContainerResponse.__name__,
               "id"             : obj.id,
               "command"        : obj.command,
               "cgroupKeyValues": obj.cgroupKeyValues
               }

    @staticmethod
    def _dict_to_class_(dic):
        assert(dic["__name__"]==ContainerResponse.__name__)
        containerResponse = ContainerResponse(id             = dic["id"],
                                              command        = dic["command"],
                                              cgroupKeyValues= dic["cgroupKeyValues"]
                                              )
        return containerResponse
 

     
