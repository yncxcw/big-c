#!/usr/bin/python

import json
import logging
import logging

log=logging.getLogger("RMDocker.YarnCommand")

class YarnCommandType:

    DEHYDRATE = 0
    RESUME    = 1
    UPDATE    = 2

class Resource:

    def __init__(self,vcores,memory):
        self.vcores = vcores
        self.memory = memory

class YarnCommand:


    def __init__(self,id,resource,type):
        self.id       = id
        self.resource = resource
        self.type     = type

    def get_id(self):
        return self.id

    def get_resource(self):
        return self.resource

    def get_type(self):
        return self.type

    @staticmethod
    def _dict_to_class_(dic):
        className = dic["__name__"]
        assert(className == "org.apache.hadoo.yarn.server.resourcemanager.dockermonitor.DockerCommand")
        resource = Resource(
                            vcores = int(dic["resource.vcores"]),
                            memory = int(dic["resource.memory"])
                           )
        yarnCommand = YarnCommand(
                                  id  = str(dic["containerId"]),
                                  type= int(dic["commandType"]),
                                  resource = resource
                                 )
        return yarnCommand

