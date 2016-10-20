# PreemptYARN-2.7.1

This project enables YARN performing fast and low-cost preemption by leveraging Docker.

##Install and compile
For compile, plesse refer BUILDING.txt for detail. Since my codebase is built on Hadoop-2.7.1, it depends on libprotoc-2.5.0(higher version may report error).

##Docker image
Please use /sequenceiq/hadoop-docker as the docker image for running task. We have tested /sequenceiq/hadoop-docker:2.4.0, and it can
both support Hadoop Mapreduce and Spark applications. For configuring YARN with docker support, please refer this:
https://hadoop.apache.org/docs/r2.7.2/hadoop-yarn/hadoop-yarn-site/DockerContainerExecutor.html

I have hacked NodeManager, so you do not need to configure mapreduce.map.env, mapreduce.reduce.env, yarn.app.mapreduce.am.env
to indicate docker images when you launch applications. Once you set your default yarn.nodemanager.container-executor.class as 
org.apache.hadoop.yarn.server.nodemanager.DockerContainerExecutor, all the containers will be launched with this executor.

##Configuration for preemption
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

This project is still under active development, if you have any questions, feel free to contact ynjassionchen@gmail.com



