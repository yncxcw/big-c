#!/usr/bin/python

import logging

from docker import Client
from container import Container
from hostStatusUpdateRequest import ContainerAction, HostUpdate,ContainerUpdate 
from hostStatusUpdateResponse import ContainerCommand, HostResponse,ContainerResponse 

log=logging.getLogger("RMDocker.LiveContainerManager")

class LiveContainerManager:


    def __init__(self,configure,host):
        ##keep all the live container information
        self.liveContainers = {}
        self.configure = configure
        ##cgroup parameters needed to access
        self.host = host

    def getLiveContainers(self):
        ##connect to container.sock
        cli = Client(version='1.20',base_url='unix://var/run/docker.sock')
        ##emulate container ps -a
        try:
            psCommandStr=cli.containers()
        except Exception as error:
            log.error("error when access docker.sock")
        ##process results with json
        containers = []
        for containerStr in psCommandStr:
            containerName  = containerStr['Names'][0][1:]
            containerId    = containerStr['Id']
            containerStatus= containerStr['Status']
            containerIamge = containerStr['Image']
            container = Container(containerId,containerName,self.configure)
            if container.isRunning() is False:
                log.error("container is not running")
                continue
            container.setStatus(containerStatus)
            container.setImage(containerIamge)
            container.addCgroups()
            containers.append(container)
        return containers
            

    def initLiveContainers(self):
        containers = self.getLiveContainers()
        for container in containers:
            self.liveContainers[container.getID()] = container
    
    def updateLiveContainers(self):
        containers = self.getLiveContainers()
        liveID   =  []
        for container in containers:
            liveID.append(container.getID())

        statusList = []

        ##add new containers
        for container in containers:
            if container.getID() not in self.liveContainers.keys():
                log.info("created containers: %s",container.getName())
                ##this is a newly set up container
                self.liveContainers[container.getID()] = container
                ##start its update thread
                container.start()
                ##construct its initial key values
                status = self.constructionContainerUpdate(container,ContainerAction.NEW)
                statusList.append(status)
                ##we have already seen this contianer before, just sent update info
            else:
                log.info("update containers %s",container.getName())
                ##read from cgroup file system to update cgroup values
                self.liveContainers[container.getID()].read()
                status = self.constructionContainerUpdate(container,ContainerAction.UPDATE)
                statusList.append(status)

        ##delete out of date containers
        idContainerToBeDeleted = []
        for containerID in self.liveContainers.keys():
            if containerID not in liveID:
                log.info("deleted containers: %s",self.liveContainers[containerID].getName())
                status = self.constructionContainerUpdate(self.liveContainers[containerID],ContainerAction.DIE)
                statusList.append(status)
                idContainerToBeDeleted.append(containerID)
        for containerID in idContainerToBeDeleted:
            del self.liveContainers[containerID]
        
        hostUpdate =  HostUpdate(self.host,statusList)

        return hostUpdate

            
    def getLiveContainerSize(self):
        return len(self.liveContainers)


    def killContainer(self,id):
        ##TODO
        pass

    def startContainer(self,id):
        ##TODO
        pass

    def updateContianers(self,containerToCgroups):
        for id in containerToCgroups.keys():
            try:
                container = self.liveContainers[id]
            except Exception as error:
                log.error("find contianer error %s",error)
            ##update key value
            if container.isRunning() is False:
                log.error("container is not running")
                return
            ##add the cgroups to contaienr's task list
            container.update(containerToCgroups[id])

         
    ##execute the command sent back from master
    def liveContainerProcess(self,hostUpdate):
        if hostUpdate is None:
            log.info("none host update")
            return
        if hostUpdate.getContainerResponses() is None:
            return

        ##store cgroup updating for one heartbeat
        containerToCgroups = {}
        for containerResponse in hostUpdate.getContainerResponses():
            ##none command
            if containerResponse.getCommand() is ContainerCommand.NONE:
                continue
            elif containerResponse.getCommand() is ContainerCommand.KILL_CONTAINER:
                ##we do it immediately
                log.info("kill command %s",containerResponse.getID())
                self.killContainer(containerResponse.getID()) 
            elif containerResponse.getCommand() is ContainerCommand.START_CONTAINER:
                ##we do it immediately
                log.info("start command %s",containerResponse.getID())
                self.startContainer(containerResponse.getID()) 
            elif containerResponse.getCommand() is ContainerCommand.UPDATE_CGROUP_PARAMETER:
                log.info("update command %s",containerResponse.getID())
                cnt_id = containerResponse.getID()
                if containerToCgroups.get(cnt_id) is None:
                    containerToCgroups[cnt_id] = []
                    containerToCgroups[cnt_id].append(containerResponse.getCgroupKeyValues())
                else:
                    containerToCgroups[cnt_id].append(containerResponse.getCgroupKeyValues())

        self.updateContianers(containerToCgroups)
                                   
    def constructionContainerUpdate(self,container,action):
        ##network flow detect(all plungable function added here)
        if self.configure.get("networkflow") is not None:
            networkflows = container.getWorkFlow()
        else:
            networkflows = None

        containerUpdate = ContainerUpdate(
                                          name=container.getName(),
                                          id=container.getID(),
                                          action=action,
                                          cgroupKeyValues=container.getCgroupKeyValues(),
                                          netflows = networkflows 
                                         )
        #print (container.getCgroupKeyValues())


        return containerUpdate


     
    def printAllContainers(self):
        for containerID in self.liveContainers.keys():
            container = self.liveContainers[containerID]
            container.printContainer()

