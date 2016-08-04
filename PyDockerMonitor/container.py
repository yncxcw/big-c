#!/usr/bin/python

import logging
import threading
from cgroup import Cgroup
from containerFlow import ContainerFlow
import os

import time

log = logging.getLogger("RMDocker.Container")

class Container(threading.Thread):
  
     
    def __init__(self,id,name,configure):
        threading.Thread.__init__(self)
        self.image = ""
        self.status= ""
        self.pid   = 0
        self.name = name
        self.id   = id
        self.testPath="/sys/fs/cgroup/memory/docker/"+str(id)
        self.configure = configure
        self.cgroups = {}
        ##task key list and its hash map
        self.task_key = []
        self.task_map = {}
        self.task_lock= threading.RLock()
        ##read in pid
        pid_path = self.testPath+"/cgroup.procs"
        try:
            file = open(pid_path)
            for line in file.readlines():
                if int(line.strip()) > self.pid:
                    self.pid = int(line.strip())
        except Exception as exce:
            log.error("get pid error %s",exce)

        ##work flow monitor
        self.flow = ContainerFlow(self.pid)


    def getWorkFlow(self):
        return self.flow.monitor()


    def getPid(self):
        return self.pid
   
    def getName(self):
        return self.name

    def getID(self):
        return self.id

    def setImage(self,image):
        self.iamge=image

    def getImage(self):
        return self.image

    def setStatus(self,status):
        self.status=status

    def getStatus(self):
        return self.status

    def isRunning(self):
        if os.path.exists(self.testPath):
            return True
        else:
            return False

    def addCgroups(self):
        for cgroupName in self.configure.get("cgroup"):
            cgroup = Cgroup(cgroupName,self.id,self.configure)
            cgroup.initialize()
            self.cgroups[cgroupName]=cgroup

    def getValue(self,name,key):
        return self.cgroups[name].get(key)


    def getCgroupKeyValues(self):
        cgroupKeyValues = {}
        for key in self.cgroups.keys():
            cgroupKeyValues[key] = self.cgroups[key].getKeyValues()
        return cgroupKeyValues


    ##a thread to execute update command
    ##the order is restricted by task_map
    ##and task_key
    def run(self):
        while(self.isRunning()):
            if len(self.task_key) == 0:
                time.sleep(1)
            else:
                name=None
                key=None
                value=None
                with self.task_lock:
                    log.info("enter thread update %s id %s",self.name,self.id)
                    log.info("task_key %s",self.task_key)
                    log.info("task_map %s",self.task_map)
                    task_item = self.task_key[0]
                    name      = task_item[0]
                    key       = task_item[1]
                    ##we should make sure if at least one value
                    ##in task_map[key]
                    value     = self.task_map[key].pop(0)
                    log.info("get name %s key %s value %s",name,key,value)
                    if len(self.task_map[key]) == 0:
                        del self.task_map[key]
                        ##each time we delete the first element of the list
                        log.info("delete key %s",key)
                        delete_item = self.task_key.pop(0)
                        #assert(delete_item[1] == key)
                        log.info("delete item %s",delete_item)
                    log.info("exit thread update %s",self.name)
                log.info("we update name %s key %s value %s",name,key,value)
                ##we do actually update and sync here
                self.updateKeyValue(name,key,value)
                self.syncKeyValue(name,key)
                time.sleep(1)
        log.info("safe exit from thread container %s",self.name)
                
        ##TODO release resource


    ##this is an one time updating for heartbeat
    ##if we found previous value for a same key, we override this value
    ##cgroup is a list of cgroups
    ##cgroup is a hash map
    ##{ 
    ##      "cpu":{
    ##           "cpu.cfs_period_us":100000,
    ##           "cpu.cfs_quota_us":1000
    ##            }
    ##}
    def update(self,cgroups):
        ##update cgroup in order, this the the order
        ##how we update cgroup
        ##delete previous key to override
        with self.task_lock:
            log.info("enter update")
            for cgroup in cgroups:
                for name in cgroup:
                    for key in cgroup[name].keys():
                        if self.task_map.get(key) is not None:
                            self.task_map[key].clear()
            ##we insert new key value
            for cgroup in cgroups:
                for name in cgroup:
                    for key,value in cgroup[name].items():
                        ##we append the new key
                        if self.task_map.get(key) is not None:
                            self.task_map[key].append(value)
                        ##it's a new key we never met before
                        else:
                            ##like(memory,memory,limite_in_bytes)
                            ##update task_key
                            log.info("append new key item name %s key %s",name,key)
                            new_key_item = (name,key)
                            self.task_key.append(new_key_item)
                            ##update task_map
                            self.task_map[key] = []
                            self.task_map[key].append(value)
            log.info("exit update %s",self.name)
    
    def updateKeyValue(self,name,key,value):
        self.cgroups[name].update(key,value)

    def read(self):
        for key in self.cgroups.keys():
            self.cgroups[key].read()
    
    def syncKeyValue(self,name,key):
        return self.cgroups[name].sync(key)     

    def getCgroupSize(self):
        return len(self.cgroups)

    def printContainer(self):
        for cgroupName in self.cgroups.keys():
            self.cgroups[cgroupName].printCgroup()
                 



