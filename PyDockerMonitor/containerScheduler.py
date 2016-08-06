#!/usr/bin/python
import logging
from hostStatusUpdateResponse import ContainerCommand, HostResponse, ContainerResponse
from hostToContainerManager import CTContainerStatus  
from collections import deque
from YarnCommand import YarnCommandType

log=logging.getLogger("RMDocker.ContainerScheduler")

MAX_BOOST=1

class ContainerScheduler:

    name = "BASE_SCHEDULER"

    def __init__(self,hostToContainerManager):
        self.hostToCommands         = {}
        self.hostToContainerManager = hostToContainerManager

    def register(self,host):
        self.hostToCommands[host]=deque()

    def deregister(self,host):
        log.info("deregister host %s from ContainerScheduler",host)
        del self.hostToCommands[host]

    ##if receive heartbeat from host, then make response
    def notify(self, host, id=None, command=None, cgroupKeyValues=None):
        ##if we receive other commands from user or other schedulers
        if id is not None:
            self.hostToCommands[host].append(ContainerScheduler._make_contaienrResponse_(id,command,cgroupKeyValues))
        else:
            pass

    ##scheduler yarn commands 
    def schedule(self,command):
        containerId = command.get_id()
        if self.hostToContainerManager.getContainerByName(containerId) is None:
            log.info("can not find container %s",containerId)
            return False
        container = self.hostToContainerManager.getContainerByName(containerId)
        if command.get_type() == YarnCommandType.DEHYDRATE:
            if container.getStatus() == CTContainerStatus.SUSPEND:
                log.info("contianer %s is suspending, can not suspend again",containerId)
                return False
            else:
                log.info("successfully suspend container %s host %s",containerId,container.getHost())
                self.suspendContainerResponse(container)
        elif command.get_type() == YarnCommandType.RESUME:
            if container.getStatus() != CTContainerStatus.SUSPEND:
                log.info("contianer %s is not suspending, can not resume again",containerId)
                return False
            else:
                log.info("successfully resume container %s host %s",containerId,container.getHost())
                self.resumContainerResponse(container)
        elif command.get_type() == YarnCommandType.UPDATE:
            ##TODO support in future release
            pass                
        return True
                      
  
    @staticmethod
    def getContainerMemoryUsage(container):
        memory = 0
        try:
            memory=int(container.getCgroupValue("memory","memory.usage_in_bytes"))
        except Exception as error:
            log.eror("keyvalue error %s",error) 
        return memory/(1024*1024)

                        
    @staticmethod
    def getContainerSwapUsage(container):
        swap = 0
        try:
            swap= int(container.getCgroupValue("memory","memory.stat").strip().split(":")[1])
        except Exception as error:
            log.error("KeyValue eror %s",error)
        return swap/(1024*1024)
   
    @staticmethod
    def getContainerMemoryLimit(container):
        usage = 0
        try:
            usage= int(container.getCgroupValue("memory","memory.limit_in_bytes"))
        except Exception as error:
            log.error("KeyValue eror %s",error)
        return usage/(1024*1024)
   


    @staticmethod
    def isToSuspend(container):
        ##get memory usage
        memory_usage  = ContainerScheduler.getContainerMemoryUsage(container)           ##get swap usage
        swap_usage    = ContainerScheduler.getContainerSwapUsage(container)
        ##get memory limit
        memory_limit  = ContainerScheduler.getContainerMemoryLimit(container) 
        ##if consume more than 500mb and memory usage is full
        if memory_usage + swap_usage > memory_limit and swap_usage >= 500: 
            log.info("container %s is swapping",container.getName())
            return True
        else:
            return False

    
    def suspendContainerResponse(self,container):
        log.info("enter suspend")
        ##set memory 1% of total memory
        memory_value = int(ContainerScheduler.getContainerMemoryLimit(container))
        old_limit = str(int(ContainerScheduler.getContainerMemoryLimit(container)))+"m"
        container.put("memory","memory.limit_in_bytes",old_limit)
        ##set cpu usage 1% of total cpu frequency,suspense cpu first
        quota = "1000"
        period= "100000"
        cgroupCpuKeyValue ={
                          "cpu"    :{
                                   "cpu.cfs_period_us"    :period,
                                   "cpu.cfs_quota_us"     :quota
                                   }

                          }
        containerCpuResponse = ContainerScheduler._make_containerResponse_(
                                                                        id             = container.getID(),
                                                                        command        = ContainerCommand.UPDATE_CGROUP_PARAMETER,
                                                                        cgroupKeyValues=cgroupCpuKeyValue
                                                                        )
        self.hostToCommands[container.getHost()].append(containerCpuResponse)
        ##suspense memory incrementally
        while memory_value > 300:
            memory_value = int(memory_value * 0.5)
            memory_value_str = str(memory_value)+"m"
            cgroupMemoryKeyValue={
                                 "memory":{
                                          "memory.limit_in_bytes":memory_value_str
                                    }
                                  }
            containerMemoryResponse = ContainerScheduler._make_containerResponse_(
                                                                                id             = container.getID(),
                                                                                command        = ContainerCommand.UPDATE_CGROUP_PARAMETER,
                                                                                cgroupKeyValues=cgroupMemoryKeyValue
                                                                               )
            self.hostToCommands[container.getHost()].append(containerMemoryResponse)

        container.setStatus(CTContainerStatus.SUSPEND)
        log.info("suspend container %s",container.getName())

    
    
    def resumContainerResponse(self,container):
        ##set memory 1% of total memory
        limit    = container.get("memory","memory.limit_in_bytes")
        ##set cpu usage 100% of total cpu frequency
        quota = "-1"
        period= "100000"
        cgroupKeyValues={"memory":{
                                   "memory.limit_in_bytes":limit 
                                   },
                        "cpu"    :{
                                   "cpu.cfs_period_us"    :period,
                                   "cpu.cfs_quota_us"     :quota
                                   }
                        }
        containerResponse = ContainerScheduler._make_containerResponse_(
                                                                        id             = container.getID(),
                                                                        command        = ContainerCommand.UPDATE_CGROUP_PARAMETER,
                                                                        cgroupKeyValues=cgroupKeyValues
                                                                        )
        log.info("resume container %s %s",container.getName(),limit)
        container.setStatus(CTContainerStatus.RUN)
        self.hostToCommands[container.getHost()].append(containerResponse)





    def boostContainerResponse(self,container,limit):
        limit=int(limit)
        log.info("boost container %s",container.getName())
        cgroupKeyValues={"memory":{"memory.limit_in_bytes":str(limit)+"m"}}
        containerResponse = ContainerScheduler._make_containerResponse_(
                                                                     id             = container.getID(),
                                                                     command        = ContainerCommand.UPDATE_CGROUP_PARAMETER,
                                                                     cgroupKeyValues=cgroupKeyValues
                                                                        )
        container.setStatus(CTContainerStatus.BOOST)
        self.hostToCommands[container.getHost()].append(containerResponse)
    

 

    def _make_hostResponse_(self,host):
        commands = []
        if len(self.hostToCommands[host]) > 0:
            log.info("we have %d commands to host %s",len(self.hostToCommands[host]),host)
            while len(self.hostToCommands[host]) > 0:
                commands.append(self.hostToCommands[host].popleft())
                
        hostResponse = HostResponse(host,commands)
        return hostResponse

    @staticmethod
    def _make_containerResponse_(id, command=None, cgroupKeyValues=None):
        containerResponse = ContainerResponse(id,command,cgroupKeyValues)
        return containerResponse  

