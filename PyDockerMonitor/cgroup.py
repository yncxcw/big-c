#!/usr/bin/python

from os import listdir
from parameter import Parameter

import logging

log=logging.getLogger("RMDocker.Cgroup")

class Cgroup:

    
    def __init__(self,name,id,configure):
        ## name of the subsystem
        self.name=name
        ## path to current subsystem
        self.path="/sys/fs/cgroup/"+name+"/docker/"+str(id)
        ## map from subsystem parameter to its class   
        self.subParameters={}
        self.configure = configure

        
    def initialize(self):
        parameters = []
        configure_parameters = self.configure.get(self.name)
        if type(configure_parameters) is list:
            parameters.extend(configure_parameters)
        else:
            parameters.append(configure_parameters)
        files=listdir(self.path)
        for parameterName in parameters:
            ##for example cpu.shares then it will start with cpu, we only configure these files.
            if parameterName in files:
                ##we pass the path fo the file under this subsystem to Parameter class 
                fileName = self.path+"/"+parameterName
                
                parameter = Parameter(name=parameterName,
                                      path=fileName,
                                      configure=self.configure)
                self.subParameters[parameterName]=parameter
            else:
                log.error("can not find file parameter %s",parameterName)
                ##TODO we can't find a file mathed with parameter log error message here
                pass
            ##read initial values
        for parameter in self.subParameters.values():
            parameter.read()
  

    def getKeyValues(self):
        keyValues = {}
        for key in self.subParameters.keys():
            keyValues[key] = self.get(key)
        return keyValues
             
    def update(self,key,value):
        self.subParameters[key].update(value)

    def get(self,key):
        return self.subParameters[key].get()

    def read(self):
        for parameter in self.subParameters.values():
            parameter.read()

    def sync(self,key):
        return self.subParameters[key].sync()

    def printCgroup(self):
        for parameter in self.subParameters.keys():
            print ("key : ", parameter, " value : ",self.get(parameter))    
