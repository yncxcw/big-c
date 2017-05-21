# BIG-C

This project enables YARN performing fast and low-cost preemption by leveraging Docker.

## Install and compile
For compile, plesse refer BUILDING.txt for detail. Since my codebase is built on Hadoop-2.7.1, it depends on libprotoc-2.5.0(higher version may report error).

## Docker image
Please use /sequenceiq/hadoop-docker as the docker image for running applications. 
We have tested /sequenceiq/hadoop-docker:2.4.0, and it can
support both Hadoop Mapreduce and Spark. For configuring YARN with docker support, please refer this:

https://hadoop.apache.org/docs/r2.7.2/hadoop-yarn/hadoop-yarn-site/DockerContainerExecutor.html

We have hacked NodeManager. Following configurations below will have all your applications (both MapReduce and Spark) running
in Docker containers: 

in yarn-site.xml
```
<property>
    <name>yarn.nodemanager.container-executor.class</name>
    <value>org.apache.hadoop.yarn.server.nodemanager.DockerContainerExecutor</value>
</property>
````
````
<property>
    <name>yarn.nodemanager.docker-container-executor.exec-name</name>
    <value>/usr/bin/docker(path to your docker)</value>
</property>
````



## Configuration for preemption
In yarn-site.xml
```
<property>
    <name>yarn.resourcemanager.scheduler.monitor.enable</name>
    <value>True</value>
</property>
```
Enable resource monitor for Capacity Scheduler.

```
<property>
    <name>yarn.resourcemanager.monitor.capacity.preemption.suspend</name>
    <value>True</value>
</property>
```
Enable suspension based preemption for Capacity Scheduler. If this option is False, then only killing based preemption will be
applied. 

In capacity-site.xml
```
<property>
    <name>yarn.scheduler.capacity.root.default.maxresumptopportunity</name>
    <value>3</value>
</property>
```
This parameter sets the SR_D(refer paper for detail) for queue root.default

In capacity-site.xml
```
<property>
    <name>yarn.scheduler.capacity.root.default.pr_number</name>
    <value>2</value>
</property>
```
This parameter sets the SR_NUM(refer paper for detail) for queue root.default

For more information, please refer our paper or contact ynjassionchen@gmail.com.



