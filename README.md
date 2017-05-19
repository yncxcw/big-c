# PreemptYARN-2.7.1

This project enables YARN performing fast and low-cost preemption by leveraging Docker.

## Install and compile
For compile, plesse refer BUILDING.txt for detail. Since my codebase is built on Hadoop-2.7.1, it depends on libprotoc-2.5.0(higher version may report error).

## Docker image
Please use /sequenceiq/hadoop-docker as the docker image for running task. We have tested /sequenceiq/hadoop-docker:2.4.0, and it can
both support Hadoop Mapreduce and Spark applications. For configuring YARN with docker support, please refer this:

https://hadoop.apache.org/docs/r2.7.2/hadoop-yarn/hadoop-yarn-site/DockerContainerExecutor.html

I have hacked NodeManager, following configurations below will have all your applications (both MapReduce and Spark) running
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
    <name>yarn.resourcemanager.monitor.capacity.preemption.suspend</name>
    <value>True</value>
</property>
```
This will enable docker-based preemption, other wise kill-based will be applied, you also need to configure yarn.resourcemanager.scheduler.monitor.enable to be true to enable default preemption mechanism in YARN.

In Capacity-site.xml
```
<property>
    <name>yarn.scheduler.capacity.root.default.maxresumptopportunity</name>
    <value>3</value>
</property>
```
This parameter configure the SR_D(refer paper for detail) for queue root.default

In Capacity-site.xml
```
<property>
    <name>yarn.scheduler.capacity.root.default.pr_number</name>
    <value>2</value>
</property>
```
This parameter configure the SR_NUM(refer paper for detail) for queue root.default

For more information, please refer our paper or contact ynjassionchen@gmail.com.



