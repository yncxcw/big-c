#!/usr/bin/python

import os
import sys
import logging

def _check_version_():
    if sys.version_info<(3,0):
        import warnings
        warnings.warn("This RMDocker is unsupported on python version less than 2.7")
        return False
    else:
        return True


def _configure_logging_(mode):
    loglevelvalue = os.environ.get("RMDOCKER_LOGLEVEL","DEBUG")
    if mode == "master":
        logfilename   = os.environ.get("RMDOCKER_LOGFILE","rmdocker_master.log")
    else:
        logfilename   = os.environ.get("RMDOCKER_LOGFILE","rmdocker_slave.log")
 

    logging.basicConfig(
                        level   = loglevelvalue,
                        filename= logfilename,
                        datefmt = "%Y-%m-%d %H:%M:%S",
                        format  = "[%(asctime)s.%(msecs)03d,%(name)s,%(levelname)s] %(message)s"

                       )

    log=logging.getLogger(__name__)
    log.info("RMdocker configured using built-in defaults,level=%s",loglevelvalue)


        
def _check_every_thing_(mode):

    _check_version_()

    _configure_logging_(mode) 
