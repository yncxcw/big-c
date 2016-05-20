#!/usr/bin/python
import logging
from hostStatusUpdateResponse import ContainerCommand, HostResponse, ContainerResponse
from hostToContainerManager import CTContainerStatus  
from collections import deque
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

    ##if receive heartbeat from host, then notify the scheduler
    def notify(self, host, id=None, command=None, cgroupKeyValues=None):
        ##if we receive other commands from user or other schedulers
        if id is not None:
            self.hostToCommands[host].append(ContainerScheduler._make_contaienrResponse_(id,command,cgroupKeyValues))
        else:
            pass
        self.schedule(host)

    ##scheduler algorithms 
    def schedule(self,host):
        hostmemory=8*1024 - 512
        containers = self.hostToContainerManager.getContainersOnHost(host)
        log.info("%d containers are on host %s",len(containers),host)
        if len(containers) == 0:
            return
        boost_containers    = []
        run_containers       = []
        suspend_containers   = []
        
        to_suspend_containers= []

        for container in containers:
            if container.getStatus() == CTContainerStatus.RUN:
                ##check we need to suspend this running container
                if ContainerScheduler.isToSuspend(container) is True:
                    log.info("add container %s to to suspend",container.getName())
                    to_suspend_containers.append(container)
                else:
                    log.info("add container %s to run",container.getName())
                    run_containers.append(container)
            elif container.getStatus() == CTContainerStatus.SUSPEND:
                log.info("container %s has been suspended",container.getName())
                suspend_containers.append(container)
            elif container.getStatus() == CTContainerStatus.BOOST:
                log.info("container %s has benn boosted",container.getName())
                boost_containers.append(container)

        ##if no contianers are swapping
        swapping_containers = len(suspend_containers) + len(to_suspend_containers)
        if  swapping_containers == 0:
            return

        log.info("swapping containers on host %s are %d",host,swapping_containers)

        used_memory = 0
        for container in run_containers:
            used_memory = used_memory+ContainerScheduler.getContainerMemoryLimit(container)
        
        unused_memory = hostmemory - used_memory


        log.info("unused memory on host is %d", unused_memory)
        ##if there exits at least on is swapping, we boost one
        if len(boost_containers) == 0:
            ##we pick one to suspend container to boost
            if len(to_suspend_containers) > 0:
                to_boost_container = to_suspend_containers.pop()
                if unused_memory > ContainerScheduler.getContainerMemoryLimit(to_boost_container):
                    log.info("boost container from to suspend")
                    self.boostContainerResponse(to_boost_container,unused_memory)
            ##all containers are suspend
            else:
                to_boost_container = suspend_containers.pop()
                if unused_memory > ContainerScheduler.getContainerMemoryLimit(to_boost_container):
                    log.info("boost container from suspend")
                    self.resumContainerResponse(to_boost_container)
                    self.boostContainerResponse(to_boost_container,unused_memory)
                
        elif len(boost_containers) == 1: 
            to_boost_container = boost_containers[0]
            if unused_memory > ContainerScheduler.getContainerMemoryLimit(to_boost_container):

                self.boostContainerResponse(to_boost_container,unused_memory)
 
        elif len(boost_containers) > 1:
            log.error("more than one is boosted")
       
        log.info("still we have %d containers to suspend",len(to_suspend_containers)) 
        ##suspend the rest       
        for container in to_suspend_containers:
            log.info("pre suspend")
            self.suspendContainerResponse(container)
              
  
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
        old_limit = str(int(ContainerScheduler.getContainerMemoryLimit(container)))+"m"
        log.info("we got old here")
        container.put("memory","memory.limit_in_bytes",old_limit)
        log.info("after put")
        limit    = "100m"
        ##set cpu usage 1% of total cpu frequency
        quota = "10000"
        period= "1000000"
        cgroupKeyValues={"memory":{
                                   "memory.limit_in_bytes":limit 
                                   },
                        "cpu"    :{
                                   "cpu.cfs_period_us"    :period,
                                   "cpu.cfs_quota_us"     :quota
                                   }
                        }
        log.info("after keyvalue")
        containerResponse = ContainerScheduler._make_containerResponse_(
                                                                        id             = container.getID(),
                                                                        command        = ContainerCommand.UPDATE_CGROUP_PARAMETER,
                                                                        cgroupKeyValues=cgroupKeyValues
                                                                        )
        container.setStatus(CTContainerStatus.SUSPEND)
        log.info("suspend container %s",container.getName())
        self.hostToCommands[container.getHost()].append(containerResponse)

    
    
    def resumContainerResponse(self,container):
        ##set memory 1% of total memory
        limit    = container.get("memory","memory.limit_in_bytes")
        ##set cpu usage 100% of total cpu frequency
        quota = "-1"
        period= "1000000"
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

