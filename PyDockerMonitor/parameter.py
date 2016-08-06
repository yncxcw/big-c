#!/usr/bin/python


import time
import logging


log=logging.getLogger("RMDocker.Parameter")

class Parameter:


    retried = 10


    def __init__(self,name,path,configure):
        self.name=name
        self.path=path
        self.changed = False
        self.value   = ""
        self.configure=configure
        self.subParameter=[]
        if self.configure.get(self.name) is None:
            return
        if type(self.configure.get(self.name)) is list:
            self.subParameter.extend(self.configure.get(self.name))
        else:
            self.subParameter.append(self.configure.get(self.name))
            

    def read(self):
        try:
            file=open(self.path)
            self.value=""
            for line in file.readlines():
                if len(line.split()) == 1:
                    self.value=line.strip()
                    break   
                elif line.split()[0].strip() in self.subParameter:
                    self.value=self.value+line.split()[0].strip()+":"+line.split()[1].strip()+" "
                else:
                    continue
        except Exception as error:
            log.error("read error %s",error)

    def update(self,value):
        self.changed=True
        self.value=value

    def sync(self):
        isSuccess = False
        if self.changed is False:
            return True
        count = self.retried
        file=None
        log.info("key %s",self.path)
        log.info("value %s",self.value)
        while count > 0:
            try:
                file=open(self.path,"w")
                file.write(str(self.value))
                file.close()
            except Exception as error:
                count = count - 1
                log.error("write value to file with error %s count %d",error,10-count)
                ##time.sleep(1)
                continue
            isSuccess = True
            break
        if isSuccess: 
            self.changed = False
        else:
            log.error("write value error")
        return isSuccess
        
    def get(self):
        return self.value        

