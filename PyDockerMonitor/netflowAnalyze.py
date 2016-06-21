#!/bin/python


class NetflowAnalyze:

    def __init__(self):
        self.containerToNet={}

    def update(self,container,netflow):
        self.containerToNet[contaienr.getID()] = netflow
