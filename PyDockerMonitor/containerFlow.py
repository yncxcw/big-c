#!/bin/python
import psutil
import logging

log=logging.getLogger("RMDocker.ContaienrFlow")

class ContainerConnect:

    def __init__(self,lport,laddr,rport,raddr,state):
        ##local port
        self.lport = lport
        ##local address
        self.laddr = laddr
        ##remote port
        self.rport = rport
        ##remote address
        self.raddr = raddr
        ##sate(LISTEN/ESTABLISHED)
        self.state = state

    def get_lport(self):
        return self.lport

    def get_laddr(self):
        return self.laddr

    def get_rport(self):
        return self.rport

    def get_raddr(self):
        return self.raddr

    def get_state(self):
        return self.state

    def equal(self,connect):
        if self.lport != connect.lport:
            return False

        if self.laddr != connect.laddr:
            return False

        if self.rport != connect.rport:
            return False

        if self.raddr != connect.raddr:
            return False

        if self.state != connect.state:
            return False

        return True

    @staticmethod
    def _class_to_dict_(obj):
        assert(isinstance(obj,ContainerConnect))
        #log.info("lport %d",obj.lport)
        #log.info("laddr %s",obj.laddr)
        #log.info("rport %d",obj.rport)
        #log.info("raddr %s",obj.raddr)
        #log.info("state %s",obj.state)
        return{
                "__name__" :ContainerConnect.__name__,
                "lport"   :str(obj.lport),
                "laddr"   :obj.laddr,
                "rport"   :str(obj.rport),
                "raddr"   :obj.raddr,
                "state"   :obj.state  
              }

    @staticmethod
    def _dict_to_class_(dic):
        assert(dic["__name__"]==ContainerConnect.__name__)
        connect = ContainerConnect(
                                   laddr = dic["laddr"],
                                   lport = int(dic["lport"]),
                                   raddr = dic["raddr"],
                                   rport = int(dic["rport"]),
                                   state = dic["state"]
                                  )
        return connect


class ContainerFlow: 

    def __init__(self,pid):
        ##monitor pid
        self.pid = pid

    def monitor(self):
        log.info("get network flow for pid %d",self.pid)
        process = None
        try:
            process = psutil.Process(self.pid)
        except Exception as excep:
            log.error("getting process error: %s",excep)

        connections = None
        try:    
            connections = process.connections()
        except Exception as excep:
            log.error("getting connection error: %s",excep)

        if connections is None or len(connections) == 0:
            return None

        containerConnects = []
        for connect in connections:
            ##iterate each connect
            ##str for ip address and int for port
            if len(connect.laddr) != 0:
                laddr = connect.laddr[0]
                lport = connect.laddr[1]
            else:
                laddr = ""
                lport = -1
            ##can be 0 for listening port
            if len(connect.raddr) != 0:
                raddr = connect.raddr[0]
                rport = connect.raddr[1]
            else:
                raddr = ""
                rport = -1

            state = connect.status
            containerConnect = ContainerConnect(
                                               lport = lport,
                                               laddr = laddr,
                                               rport = rport,
                                               raddr = raddr,
                                               state = state
                                               )
            containerConnects.append(containerConnect)
        return containerConnects


