#!/bin/python
from containerFlow import ContainerConnect
import logging

log = logging.getLogger("RMDocker.NetflowAnalyze")

class NetflowAnalyze:

    def __init__(self):
        self.count  =  0
        self.containerToNet={}
        self.file = open("network.log","w")


    def close(self):
        self.file.close()

    def netEquals(self,old_list,new_list):
        ##we ignore the first update
        if old_list is None or new_list is None:
            return True

        if len(old_list) != len(new_list):
            return False
        size = 0
        
        for old in old_list:
            for new in new_list:
                if old.equal(new):
                    size = size + 1

        if size == len(new_list):
            return True
        else:
            return False
        
    def update(self,container,netflow):
        ##nothing need to update
        if len(netflow) == 0:
            return 

        old_flow = self.containerToNet.get(container)
        ##we update
        self.containerToNet[container] = netflow
        #if  self.netEquals(old_flow,netflow):
        #    ##We do nothing, because network status remains unchanged
        #    return

        if self.count < 10:
            #log.info("not running long enough %d",self.count)
            self.count = self.count + 1
            return

        self.count = 0
        ##we analysi#s and write to the log
        self.analysis()


    def match_container(self,container_l,container_r):
        if container_l == container_r:
            return False
        for connect_l in self.containerToNet.get(container_l):
            if connect_l.get_state() != "ESTABLISHED":
                continue
            for connect_r in self.containerToNet.get(container_r):
                if connect_r.get_state() != "ESTABLISHED":
                    continue
                l_addr = connect_l.get_raddr()
                l_port = connect_l.get_rport()
                r_addr = connect_r.get_laddr()
                r_port = connect_r.get_lport()
                                
                ##container_l to container_r()
                if (r_addr == l_addr) and (r_port == l_port):
                    #log.info("l_addr %s",l_addr)
                    #log.info("l_port %d",l_port)
                    #log.info("r_addr %s",r_addr)
                    #log.info("r_port %d",r_port)
                    return True

        return False


    def analysis(self):
        self.file.write("-----start analyzing-----\n")
        container_list = self.containerToNet.keys()
        for container_s in container_list:
            self.file.write("source   "+container_s.split("_")[-1]+"\n")
            dest=""
            for container_r in container_list:
                if self.match_container(container_s,container_r):
                    log.info("a2")
                    log.info("s: %s",container_s)
                    log.info("r: %s",container_r)
                    #log.info(container_r.split("_")[-1]+"   ")
                    dest=dest+container_r.split("_")[-1]+" "
                else:
                    continue
            self.file.write(dest+"\n")
        self.file.write("-----end  analyzing-----\n")
        self.file.flush()



