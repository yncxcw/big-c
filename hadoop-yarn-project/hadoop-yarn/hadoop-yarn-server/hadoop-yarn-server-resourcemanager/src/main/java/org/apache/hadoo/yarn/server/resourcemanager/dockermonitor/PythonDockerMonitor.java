package org.apache.hadoo.yarn.server.resourcemanager.dockermonitor;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import net.razorvine.pyro.*;

public class PythonDockerMonitor extends AbstractDockerMonitor {
	
	 private static final Log LOG = LogFactory.getLog(AbstractDockerMonitor.class);
	//name server which should be launched by python
	private NameServerProxy nameServerProxy = null;
	//remote object which presents the python docker monitor daemon
	private PyroProxy pyroProxy = null;
	//remote object name
    private String pyClassName;
	
	@Override
	public void Init(Configuration conf) {
		super.Init(conf);
		//TODO be careful for this, will cause serious error if misconfigured
		this.pyClassName = conf.get(YarnConfiguration.DOCKER_PYTHON_RPC_OBJECT, 
				                             YarnConfiguration.DEFAULT_DOCKER_PYTHON_RPC_OBJECT);  
		
		//fisrt we try to locate name server
		try{
			nameServerProxy = NameServerProxy.locateNS(null);
		}catch(IOException e){
			LOG.info("failed to locate name server"+e.getMessage());
			return;
		}
		
		//second we try to locate remote Docker daemon
		try{
			pyroProxy = new PyroProxy(nameServerProxy.lookup(pyClassName));
		}catch(IOException e){
			LOG.info("ns loop up exception"+e.getMessage());
			return;
		}
		
		this.isWorking = true;
		LOG.info("initially pythonDockerMonitor successfully");
	}

	@Override
	public boolean ExecuteCommand(DockerCommand command) {
		if(!isWorking){
			LOG.info("python docker monitor is not working");
		    return false;	
		}
		Map<String,String> commandMap= DockerCommand.commandToMap(command);
		
		boolean result = false;
		try{
			result=(boolean) pyroProxy.call("containerCommand", commandMap);
		}catch(IOException e){
			LOG.info("call remote object exception at "+command.getType()+"container :"+command.getContainerId());
			nameServerProxy.close();
			pyroProxy.close();
		}
		return result;
	}

}
