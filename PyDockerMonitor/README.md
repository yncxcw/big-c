# MRDocker

### License
MRDocker is licensed under the Apache License, Version 2.0.
Each source code file has full license information.

###Introduction
MRDOcker is a Docker controller which was designed to mitigate memory pressure in Big Data application(This is why this project is called MR). Since Docker is one of the most popular container implementation and is widely used to host different applications. So we design and implement MRDocker as a central container manager to detects and releif memory pressure.

Through a lot of experiments, we have observed that a lot of Hadoop or Spark applications suffer from "out of memory". For example , for Term-vecot running in Hadoop, we usualy configure JVM heap size for reduce task as 2GB. But It may consume as high as 6GB JVM heap size when running with 6GB input file and results in job failure. However if we configure a large JVM heap size for all application running in the cluster, it may eleminate "out of memory" but significantly reduce cluster efficiency.

Since Docker container could limit the processes running in a container through CGroup. We leverage Docker to mitigate memory pressure by configuring JVM with a large heap size where limiting its memory resource on Docker.


###Usage 
Our first version of this project doesn't have any dependencies with any current popular framework. We recommend you to use Hadoop-2.7.1 and configure it to run with Docker as its default executor. MRDocker runs with Master-slave mode. The master collect metrcts of running Docker running on slave nodes and perform scheduling. While slaves nodes monitor running containers on this node.

1. add all slave nodes host name or ip address to  /sbin/slaves
2. configure /conf/config, notice that you can add cgroup metrics here when monitor container
3. use /sbin/start-all.sh and /sbin/stop-all.sh to start and stop cluster.


###Dependecy
1. Python3 is required 
2. make sure you have installed Pyro4 RPC library


###Contact
if you have any problems, please contact author:ynjassionchen@gmail.com

