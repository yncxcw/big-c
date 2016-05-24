package org.apache.hadoo.yarn.server.resourcemanager.dockermonitor;

import java.io.IOException;
import java.util.Map;

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
	public void Init(RMContext rmContext) {
		
		//TODO add this option to configure files
	    this.pyClassName = "container.master";
		this.rmContext = rmContext;
		
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
