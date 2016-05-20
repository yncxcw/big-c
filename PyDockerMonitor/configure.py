#/usr/bin/python

import serpent
import logging


log=logging.getLogger("RMDocker.Configure")

class Configure:

    confs={}

    confFile="../conf/config"

    ##initialize with file name and read in file  
    def __init__(self,confFile=None,confs=None):
        if confFile is not None:
            self.confFile = confFile
        if confs is not None:
            self.confs = confs
        
    def initialize(self):
        try:
            file = open(self.confFile,"r")
            lines = file.readlines()
            for line in lines:
                #this line is commen
                if line.startswith("#"):
                    pass
                ##this line does not follow typical configuration
                elif "=" not in line:
                    pass
                else:
                    key   = line.split("=")[0].strip()
                    value = line.split("=")[1].strip()
                    ##if value contains more options
                    if ","in value:
                        value = value.split(",")
                        value = list(map(lambda x:x.strip(),value))
                    
                    self.confs[key] = value
           		
        except (IOError,OSError) as error:
	        log.error("error during initialize configure %s",error)	
	        return False
        return True		    
	    		
    ## get configuration value corresponding to key
    def get(self,key):
        try:
            if self.confs.get(key) is not None:
                return self.confs[key]
            else:
                return None
        except KeyError as error:
	        log.error("error when try to get by key",error)
        return None		

    def addConf(self,key,value=None):
	    self.confs[key] = value

   
    def printConf(self):
        print (self.confs)
        print (self.confFile)

	
    @staticmethod
    def serilized(configure):
        confs    = configure.confs
        confFile = configure.confFile

        byteString = serpent.dumps(confs,indent=True)
        byteString = byteString + "####"+serpent.dumps(confFile,indent=True)


        return byteString
    

    @staticmethod
    def deserilized(deconfigure):
        confs    = serpent.loads(deconfigure.split("###")[0])
        confFile = serpent.loads(deconfigure.split("###")[1])
        configure = Configure(confs,confFile)
        return configure

        
  

##for test only

if __name__=="__main__":

    configure1 = Configure()
    configure1.initialize()
    print (configure1.get("cgroup"))

    
