/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.hash.Hash;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingEditPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerPreemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerPreemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.PreemptableResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class implement a {@link SchedulingEditPolicy} that is designed to be
 * paired with the {@code CapacityScheduler}. At every invocation of {@code
 * editSchedule()} it computes the ideal amount of resources assigned to each
 * queue (for each queue in the hierarchy), and determines whether preemption
 * is needed. Overcapacity is distributed among queues in a weighted fair manner,
 * where the weight is the amount of guaranteed capacity for the queue.
 * Based on this ideal assignment it determines whether preemption is required
 * and select a set of containers from each application that would be killed if
 * the corresponding amount of resources is not freed up by the application.
 *
 * If not in {@code observeOnly} mode, it triggers preemption requests via a
 * {@link ContainerPreemptEvent} that the {@code ResourceManager} will ensure
 * to deliver to the application (or to execute).
 *
 * If the deficit of resources is persistent over a long enough period of time
 * this policy will trigger forced termination of containers (again by generating
 * {@link ContainerPreemptEvent}).
 */
public class ProportionalCapacityPreemptionPolicy implements SchedulingEditPolicy {

  private static final Log LOG =
    LogFactory.getLog(ProportionalCapacityPreemptionPolicy.class);

  /** If true, run the policy but do not affect the cluster with preemption and
   * kill events. */
  public static final String OBSERVE_ONLY =
      "yarn.resourcemanager.monitor.capacity.preemption.observe_only";
  /** Time in milliseconds between invocations of this policy */
  public static final String MONITORING_INTERVAL =
      "yarn.resourcemanager.monitor.capacity.preemption.monitoring_interval";
  /** Time in milliseconds between requesting a preemption from an application
   * and killing the container. */
  public static final String WAIT_TIME_BEFORE_KILL =
      "yarn.resourcemanager.monitor.capacity.preemption.max_wait_before_kill";
  /** Maximum percentage of resources preempted in a single round. By
   * controlling this value one can throttle the pace at which containers are
   * reclaimed from the cluster. After computing the total desired preemption,
   * the policy scales it back within this limit. */
  public static final String TOTAL_PREEMPTION_PER_ROUND =
      "yarn.resourcemanager.monitor.capacity.preemp .tion.total_preemption_per_round";
  /** Maximum amount of resources above the target capacity ignored for
   * preemption. This defines a deadzone around the target capacity that helps
   * prevent thrashing and oscillations around the computed target balance.
   * High values would slow the time to capacity and (absent natural
   * completions) it might prevent convergence to guaranteed capacity. */
  public static final String MAX_IGNORED_OVER_CAPACITY =
    "yarn.resourcemanager.monitor.capacity.preemption.max_ignored_over_capacity";
  /**
   * Given a computed preemption target, account for containers naturally
   * expiring and preempt only this percentage of the delta. This determines
   * the rate of geometric convergence into the deadzone ({@link
   * #MAX_IGNORED_OVER_CAPACITY}). For example, a termination factor of 0.5
   * will reclaim almost 95% of resources within 5 * {@link
   * #WAIT_TIME_BEFORE_KILL}, even absent natural termination. */
  public static final String NATURAL_TERMINATION_FACTOR =
      "yarn.resourcemanager.monitor.capacity.preemption.natural_termination_factor";
  
  public static final String IS_SUPEND_ENABLED = 
	  "yarn.resourcemanager.monitor.capacity.preemption.suspend";
  
  public static final String IS_NAIVE_EENABLED =
	  "yarn.resourcemanager.monitor.capacity.preemption.naive";
  

  // the dispatcher to send preempt and kill events
  public EventHandler<ContainerPreemptEvent> dispatcher;

  private final Clock clock;
  private double maxIgnoredOverCapacity;
  private long maxWaitTime;
  private CapacityScheduler scheduler;
  private long monitoringInterval;
  private final Map<RMContainer,Long> preempted =
    new HashMap<RMContainer,Long>();
  private static ResourceCalculator rc;
  private float percentageClusterPreemptionAllowed;
  private double naturalTerminationFactor;
  private boolean observeOnly;
  private Map<NodeId, Set<String>> labels;
  private static boolean isSuspended;
  private static boolean isNaive;
  private static boolean isTestOnlyCpu;
  private static boolean isTest;
  private static int testNumber;
  //at when the container should be preempted in seconds
  private static int testTime;

  public ProportionalCapacityPreemptionPolicy() {
    clock = new SystemClock();
  }

  public ProportionalCapacityPreemptionPolicy(Configuration config,
      EventHandler<ContainerPreemptEvent> dispatcher,
      CapacityScheduler scheduler) {
    this(config, dispatcher, scheduler, new SystemClock());
  }

  public ProportionalCapacityPreemptionPolicy(Configuration config,
      EventHandler<ContainerPreemptEvent> dispatcher,
      CapacityScheduler scheduler, Clock clock) {
    init(config, dispatcher, scheduler);
    this.clock = clock;
  }

  public void init(Configuration config,
      EventHandler<ContainerPreemptEvent> disp,
      PreemptableResourceScheduler sched) {
    LOG.info("Preemption monitor:" + this.getClass().getCanonicalName());
    assert null == scheduler : "Unexpected duplicate call to init";
    if (!(sched instanceof CapacityScheduler)) {
      throw new YarnRuntimeException("Class " +
          sched.getClass().getCanonicalName() + " not instance of " +
          CapacityScheduler.class.getCanonicalName());
    }
    dispatcher = disp;
    scheduler = (CapacityScheduler) sched;
    maxIgnoredOverCapacity = config.getDouble(MAX_IGNORED_OVER_CAPACITY, 0.1);
    naturalTerminationFactor =
      config.getDouble(NATURAL_TERMINATION_FACTOR, 0.2);
    maxWaitTime = config.getLong(WAIT_TIME_BEFORE_KILL, 0);
    monitoringInterval = config.getLong(MONITORING_INTERVAL, 3000);
    percentageClusterPreemptionAllowed =
      config.getFloat(TOTAL_PREEMPTION_PER_ROUND, (float) 0.1);
    observeOnly = config.getBoolean(OBSERVE_ONLY, false);
    isSuspended = config.getBoolean(IS_SUPEND_ENABLED, true);
    isNaive      = scheduler.getConfiguration().getNaive("root");
    LOG.info("isNaive:"+isNaive);
    isTest       = scheduler.getConfiguration().getTest("root");
    LOG.info("isTest: "+isTest);
    isTestOnlyCpu= scheduler.getConfiguration().getTestOnlyCpu("root");
    LOG.info("isTestOnlyCpu:  "+isTestOnlyCpu);
    testNumber   = scheduler.getConfiguration().getTestNumber("root");
    LOG.info("testNumber:"+testNumber);
    testTime     = scheduler.getConfiguration().getTestTime("root");
    LOG.info("testTime:  "+testTime);
    
    LOG.info("isSuspenpded init: "+isSuspended);
    rc = scheduler.getResourceCalculator();
    labels = null;
  }
  
  @VisibleForTesting
  public ResourceCalculator getResourceCalculator() {
    return rc;
  }

  @Override
  public void editSchedule() {
    CSQueue root = scheduler.getRootQueue();
    Resource clusterResources = Resources.clone(scheduler.getClusterResource());
    clusterResources = getNonLabeledResources(clusterResources);
    setNodeLabels(scheduler.getRMContext().getNodeLabelManager()
        .getNodeLabels());
    containerBasedPreemptOrKill(root, clusterResources);
  }

  /**
   * Setting Node Labels
   * 
   * @param nodelabels
   */
  public void setNodeLabels(Map<NodeId, Set<String>> nodelabels) {
    labels = nodelabels;
  }

  /**
   * This method returns all non labeled resources.
   * 
   * @param clusterResources
   * @return Resources
   */
  private Resource getNonLabeledResources(Resource clusterResources) {
    RMContext rmcontext = scheduler.getRMContext();
    RMNodeLabelsManager lm = rmcontext.getNodeLabelManager();
    Resource res = lm.getResourceByLabel(RMNodeLabelsManager.NO_LABEL,
        clusterResources);
    return res == null ? clusterResources : res;
  }
  
  /**
   * This method selects and tracks containers to be preempted. If a container
   * is in the target list for more than maxWaitTime it is killed.
   *
   * @param root the root of the CapacityScheduler queue hierarchy
   * @param clusterResources the total amount of resources in the cluster
   */
  private void containerBasedPreemptOrKill(CSQueue root,
      Resource clusterResources) {

    // extract a summary of the queues from scheduler
    TempQueue tRoot;
    synchronized (scheduler) {
      tRoot = cloneQueues(root, clusterResources);
    }

    // compute the ideal distribution of resources among queues
    // updates cloned queues state accordingly
    tRoot.idealAssigned = tRoot.guaranteed;
    Resource totalPreemptionAllowed = Resources.multiply(clusterResources,
        percentageClusterPreemptionAllowed);
    List<TempQueue> queues =
      recursivelyComputeIdealAssignment(tRoot, totalPreemptionAllowed);

    // based on ideal allocation select containers to be preempted from each
    // queue and each application
    Map<ApplicationAttemptId,Map<RMContainer,Resource>> toPreempt =
        getContainersToPreempt(queues, clusterResources);
    
    LOG.info("toPreempt size: "+toPreempt.size());

   if (LOG.isDebugEnabled()) 
    {  
    logToCSV(queues);
    }

    // if we are in observeOnly mode return before any action is taken
    if (observeOnly) {
      return;
    }

    // preempt (or kill) the selected containers
    for (Map.Entry<ApplicationAttemptId,Map<RMContainer,Resource>> e
         : toPreempt.entrySet()) {
      for (Map.Entry<RMContainer,Resource> cr : e.getValue().entrySet()) {
        // if we tried to preempt this for more than maxWaitTime
    	RMContainer container = cr.getKey();
    	Resource  resource  = cr.getValue();
        if (preempted.get(container) != null &&
            preempted.get(container) + maxWaitTime < clock.getTime()) {
          // suspend it
            
          if(isSuspended){
             dispatcher.handle(new ContainerPreemptEvent(e.getKey(), container,
                ContainerPreemptEventType.SUSPEND_CONTAINER,resource));
                LOG.info("get container "+container.getContainerId()+" to suspend resource is "
                     +resource);
          }else{
        	  if(isNaive){
        	dispatcher.handle(new ContainerPreemptEvent(e.getKey(), container,
        	                ContainerPreemptEventType.SUSPEND_CONTAINER,container.getContainer().getResource()));
        	    LOG.info("get container "+container.getContainerId()+" to suspend resource is "
        	                     +resource); 
        		  
        	  }else{
             dispatcher.handle(new ContainerPreemptEvent(e.getKey(), container,
                ContainerPreemptEventType.KILL_CONTAINER,container.getContainer().getResource())); 
               LOG.info("get container "+container.getContainerId()+" to kill resource is "
                     +resource);
        	  }
          }
          preempted.remove(container);
          
        } else {
           if(isTest){
        	   dispatcher.handle(new ContainerPreemptEvent(e.getKey(), container,
                       ContainerPreemptEventType.SUSPEND_CONTAINER,resource));
   	           LOG.info("get container "+container.getContainerId()+" to suspend resource is "
   	                     +resource);  
           }
          //otherwise just send preemption events
          dispatcher.handle(new ContainerPreemptEvent(e.getKey(), container,
                ContainerPreemptEventType.PREEMPT_CONTAINER,resource));
          if (preempted.get(container) == null) {
            preempted.put(container, clock.getTime());
          }
        }
      }
    }

    // Keep the preempted list clean
    for (Iterator<RMContainer> i = preempted.keySet().iterator(); i.hasNext();){
      RMContainer id = i.next();
      // garbage collect containers that are irrelevant for preemption
      if (preempted.get(id) + 2 * maxWaitTime < clock.getTime()) {
        i.remove();
      }
    }
  }

  /**
   * This method recursively computes the ideal assignment of resources to each
   * level of the hierarchy. This ensures that leafs that are over-capacity but
   * with parents within capacity will not be preempted. Preemptions are allowed
   * within each subtree according to local over/under capacity.only return leaf nodes for this function
   * 把所有的子queue全放在一个List里面，这里并不去区分层级关系
   * @param root the root of the cloned queue hierachy
   * @param totalPreemptionAllowed maximum amount of preemption allowed
   * @return a list of leaf queues updated with preemption targets
   */
  private List<TempQueue> recursivelyComputeIdealAssignment(
      TempQueue root, Resource totalPreemptionAllowed) {
    List<TempQueue> leafs = new ArrayList<TempQueue>();
    if (root.getChildren() != null &&
        root.getChildren().size() > 0) {
      // compute ideal distribution at this level
      computeIdealResourceDistribution(rc, root.getChildren(),
          totalPreemptionAllowed, root.idealAssigned);
      // compute recursively for lower levels and build list of leafs
      for(TempQueue t : root.getChildren()) {
        leafs.addAll(recursivelyComputeIdealAssignment(t, totalPreemptionAllowed));
      }
    } else {
      // we are in a leaf nothing to do, just return yourself
      return Collections.singletonList(root);
    }
    return leafs;
  }

  /**
   * This method computes (for a single level in the tree, passed as a {@code
   * List<TempQueue>}) the ideal assignment of resources. This is done
   * recursively to allocate capacity fairly across all queues with pending
   * demands. It terminates when no resources are left to assign, or when all
   * demand is satisfied.
   *
   * @param rc resource calculator
   * @param queues a list of cloned queues to be assigned capacity to (this is
   * an out param)
   * @param totalPreemptionAllowed total amount of preemption we allow
   * @param tot_guarant the amount of capacity assigned to this pool of queues
   */
  private void computeIdealResourceDistribution(ResourceCalculator rc,
      List<TempQueue> queues, Resource totalPreemptionAllowed, Resource tot_guarant) {

    // qAlloc tracks currently active queues (will decrease progressively as
    // demand is met)
    List<TempQueue> qAlloc = new ArrayList<TempQueue>(queues);
    // unassigned tracks how much resources are still to assign, initialized
    // with the total capacity for this set of queues
    Resource unassigned = Resources.clone(tot_guarant);
    
    // group queues based on whether they have non-zero guaranteed capacity
    Set<TempQueue> nonZeroGuarQueues = new HashSet<TempQueue>();
    Set<TempQueue> zeroGuarQueues = new HashSet<TempQueue>();

    for (TempQueue q : qAlloc) {
      if (Resources
          .greaterThan(rc, tot_guarant, q.guaranteed, Resources.none())) {
        nonZeroGuarQueues.add(q);
      } else {
        zeroGuarQueues.add(q);
      }
    }

    // first compute the allocation as a fixpoint based on guaranteed capacity
    computeFixpointAllocation(rc, tot_guarant, nonZeroGuarQueues, unassigned,
        false);

    // if any capacity is left unassigned, distributed among zero-guarantee 
    // queues uniformly (i.e., not based on guaranteed capacity, as this is zero)
    if (!zeroGuarQueues.isEmpty()
        && Resources.greaterThan(rc, tot_guarant, unassigned, Resources.none())) {
      computeFixpointAllocation(rc, tot_guarant, zeroGuarQueues, unassigned,
          true);
    }
    //we still have resource left, we set fast resumtion option
    if(unassigned.getMemory() > 0 && unassigned.getVirtualCores() > 0){
     
    	for (TempQueue t:queues) {
    	   Resource currentPrempted =  scheduler.getQueue(t.queueName).getPreemptedResource();
    	   if(Resources.greaterThan(rc,tot_guarant, currentPrempted,Resources.none())){
    		    LOG.info("set "+t.queueName+" fast preempted resource: "+currentPrempted+" avail: "+unassigned);
    	        scheduler.getQueue(t.queueName).setFastResumption(true);
    	   }
        }
    }
    // based on ideal assignment computed above and current assignment we derive
    // how much preemption is required overall,this is resource that are preemptable
    Resource totPreemptionNeeded = Resource.newInstance(0, 0);
    for (TempQueue t:queues) {
      if (Resources.greaterThan(rc, tot_guarant, t.current, t.idealAssigned)) {
    	   Resource currentPrempted =  scheduler.getQueue(t.queueName).getPreemptedResource();
    	   scheduler.getQueue(t.queueName).setFastResumption(false); 
    	   LOG.info("set "+t.queueName+" slow preempted resource: "+currentPrempted);
           Resources.addTo(totPreemptionNeeded,
           Resources.subtract(t.current, t.idealAssigned));
      }
    }

    // if we need to preempt more than is allowed, compute a factor (0<f<1)
    // that is used to scale down how much we ask back from each queue
    float scalingFactor = 1.0F;
    if (Resources.greaterThan(rc, tot_guarant,
          totPreemptionNeeded, totalPreemptionAllowed)) {
       scalingFactor = Resources.divide(rc, tot_guarant,
           totalPreemptionAllowed, totPreemptionNeeded);
    }
    
    // assign to each queue the amount of actual preemption based on local
    // information of ideal preemption and scaling factor
    for (TempQueue t : queues) {
      t.assignPreemption(scalingFactor, rc, tot_guarant);
    }
    if (LOG.isDebugEnabled()) {
      long time = clock.getTime();
      for (TempQueue t : queues) {
        LOG.debug(time + ": " + t);
      }
    }

  }
  
  /**
   * Given a set of queues compute the fix-point distribution of unassigned
   * resources among them. As pending request of a queue are exhausted, the
   * queue is removed from the set and remaining capacity redistributed among
   * remaining queues. The distribution is weighted based on guaranteed
   * capacity, unless asked to ignoreGuarantee, in which case resources are
   * distributed uniformly.
   */
  private void computeFixpointAllocation(ResourceCalculator rc,
      Resource tot_guarant, Collection<TempQueue> qAlloc, Resource unassigned, 
      boolean ignoreGuarantee) {
    // Prior to assigning the unused resources, process each queue as follows:
    // If current > guaranteed, idealAssigned = guaranteed + untouchable extra
    // Else idealAssigned = current;
    // Subtract idealAssigned resources from unassigned.
    // If the queue has all of its needs met (that is, if 
    // idealAssigned >= current + pending), remove it from consideration.
    // Sort queues from most under-guaranteed to most over-guaranteed.
    TQComparator tqComparator = new TQComparator(rc, tot_guarant);
    PriorityQueue<TempQueue> orderedByNeed =
                                 new PriorityQueue<TempQueue>(10,tqComparator);
    for (Iterator<TempQueue> i = qAlloc.iterator(); i.hasNext();) {
      TempQueue q = i.next();
      if (Resources.greaterThan(rc, tot_guarant, q.current, q.guaranteed)) {
        q.idealAssigned = Resources.add(q.guaranteed, q.untouchableExtra);
      } else {
        q.idealAssigned = Resources.clone(q.current);
      }
      
      LOG.info("queue : "+q.queueName+" idealAssigned: "+q.idealAssigned);
      
      Resources.subtractFrom(unassigned, q.idealAssigned);
      // If idealAssigned < (current + pending), q needs more resources, so
      // add it to the list of underserved queues, ordered by need.
      Resource curPlusPend = Resources.add(q.current, q.pending);
     
      if (Resources.lessThan(rc, tot_guarant, q.idealAssigned, curPlusPend)) {
        orderedByNeed.add(q);
      }
    }
    
    //set DRF resource for each queue;
    for (Iterator<TempQueue> i = qAlloc.iterator(); i.hasNext();) {
        TempQueue q = i.next();
        q.dominantResource = Resources.ComputeDominantResurce(unassigned,q.pending);
        LOG.info("queue: "+q.queueName+" dominant resource: "+q.dominantResource);
    }

    //assign all cluster resources until no more demand, or no resources are left
    while (!orderedByNeed.isEmpty()
       && Resources.greaterThan(rc,tot_guarant, unassigned,Resources.none())) {
      Resource wQassigned = Resource.newInstance(0, 0);
      // we compute normalizedGuarantees capacity based on currently active
      // queues
      resetCapacity(rc, unassigned, orderedByNeed, ignoreGuarantee);

      // For each underserved queue (or set of queues if multiple are equally
      // underserved), offer its share of the unassigned resources based on its
      // normalized guarantee. After the offer, if the queue is not satisfied,
      // place it back in the ordered list of queues, recalculating its place
      // in the order of most under-guaranteed to most over-guaranteed. In this
      // way, the most underserved queue(s) are always given resources first.
      Collection<TempQueue> underserved =
          getMostUnderservedQueues(orderedByNeed, tqComparator);
      for (Iterator<TempQueue> i = underserved.iterator(); i.hasNext();) {
        TempQueue sub = i.next();
        //the share of this queue based on unassigned resource
        LOG.info("computeFixpointAllocation inloop unused resource: "+unassigned);
        Resource wQavail = Resources.multiplyAndNormalizeUp(rc,
            unassigned, sub.normalizedGuarantee, Resource.newInstance(1, 1)); 
        LOG.info("computeFixpointAllocation inloop wQavail resource: "+wQavail+" normaliazedGuarantee: "+ sub.normalizedGuarantee); 
        Resource wQidle = sub.offer(wQavail, rc, tot_guarant);
        Resource wQdone = Resources.subtract(wQavail, wQidle);

        //wQdone is 0 说明 说明wQavail已经被分配完
        if (Resources.greaterThan(rc, tot_guarant,
              wQdone, Resources.none())) {
          // The queue is still asking for more. Put it back in the priority
          // queue, recalculating its order based on need.
          orderedByNeed.add(sub);
        }
        Resources.addTo(wQassigned, wQdone);
      }
     
      Resources.subtractFrom(unassigned, wQassigned);
    }
    
   
  }

  // Take the most underserved TempQueue (the one on the head). Collect and
  // return the list of all queues that have the same idealAssigned
  // percentage of guaranteed. the less this value, the more underserved this queue
  protected Collection<TempQueue> getMostUnderservedQueues(
      PriorityQueue<TempQueue> orderedByNeed, TQComparator tqComparator) {
    ArrayList<TempQueue> underserved = new ArrayList<TempQueue>();
    while (!orderedByNeed.isEmpty()) {
      TempQueue q1 = orderedByNeed.remove();
      underserved.add(q1);
      TempQueue q2 = orderedByNeed.peek();
      // q1's pct of guaranteed won't be larger than q2's. If it's less, then
      // return what has already been collected. Otherwise, q1's pct of
      // guaranteed == that of q2, so add q2 to underserved list during the
      // next pass.
      if (q2 == null || tqComparator.compare(q1,q2) < 0) {
        return underserved;
      }
    }
    return underserved;
  }

  /**
   * Computes a normalizedGuaranteed capacity based on active queues
   * @param rc resource calculator
   * @param clusterResource the total amount of resources in the cluster
   * @param queues the list of queues to consider
   */
  private void resetCapacity(ResourceCalculator rc, Resource clusterResource,
      Collection<TempQueue> queues, boolean ignoreGuar) {
    float activeCap = 0;
    
    if (ignoreGuar) {
      for (TempQueue q : queues) {
        q.normalizedGuarantee = (float)  1.0f / ((float) queues.size());
      }
    } else {
      for (TempQueue q : queues) {
        activeCap = activeCap + q.guaranteedRatio;
      }
      for (TempQueue q : queues) {
        q.normalizedGuarantee = q.guaranteedRatio/activeCap;
      }
    }
  }
  
  /**
   * Randomly preempt container
   * 
   * @param queues set of leaf queues to preempt from
   * @param clusterResource total amount of cluster resources
   * @return a map of applciationID to set of containers to preempt
   */

  private void getContainersToPreemptForTest(Map<ApplicationAttemptId,Map<RMContainer,Resource>> preemptMap,
		  List<TempQueue> queues, Resource clusterResource){
	 
	  
	  for (TempQueue qT : queues) {
		  if (qT.preemptionDisabled && qT.leafQueue != null) {
			  continue;
		  }
		  synchronized (qT.leafQueue) {
			  //what is the descending order
	          NavigableSet<FiCaSchedulerApp> ns = 
	              (NavigableSet<FiCaSchedulerApp>) qT.leafQueue.getApplications();
	         Iterator<FiCaSchedulerApp> desc = ns.descendingIterator();
	         while (desc.hasNext()) {
	             FiCaSchedulerApp fc = desc.next();
	             //this app has finish the test
	             if(fc.getTestDone()){
	            	 continue;
	             }
	             List<RMContainer> containers =
	            	      new ArrayList<RMContainer>(((FiCaSchedulerApp)fc).getUnPreemtedContainers());
	             Map<RMContainer,Resource> containerToResource = new HashMap<RMContainer,Resource>();
	             RMContainer amContainer=null;
	             //find am container
	             for(RMContainer rm:containers){
	            	 if(rm.isAMContainer()){
	            		 amContainer = rm;
	            		 break;
	            	 } 
	             }
	             
	             if(amContainer == null){
	            	 continue;
	             }
	             long currentTime = System.currentTimeMillis();
	             //if we reach the point, we the perform preemption
	            
	             if((currentTime - amContainer.getCreationTime())/1000 > testTime){
	            	 //only preempt cpu
	            	
	            	  for(RMContainer rm:containers){
	            			if(rm.isAMContainer()){
	            				continue;
	            		    } 
	            		Resource prResource = rm.getSRResourceUnit();
	            		if(isTestOnlyCpu){
	            			 prResource.setMemory(0);
	            		}
	            		Resources.multiplyTo(prResource, testNumber);
	            		LOG.info("test preempt "+rm.getContainerId()+" resource"+prResource);
	            		containerToResource.put(rm, prResource);
	            	  }
	           	   }
	              if(containerToResource.size() > 0){
	            	  fc.setTestDone();
	            	  LOG.info(fc.getApplicationAttemptId()+" test is done");
	            	  LOG.info("test put preempt map "+fc.getApplicationAttemptId()+"size:  "+containerToResource.size());
	            	  preemptMap.put(fc.getApplicationAttemptId(), containerToResource);  
	              }
	            }//end wile    
	         }//end synchronized  
		  }//end for
	  
	}
  
  
  /**
   * Based a resource preemption target drop reservations of containers and
   * if necessary select containers for preemption from applications in each
   * over-capacity queue. It uses {@link #NATURAL_TERMINATION_FACTOR} to
   * account for containers that will naturally complete.
   *
   * @param queues set of leaf queues to preempt from
   * @param clusterResource total amount of cluster resources
   * @return a map of applciationID to set of containers to preempt
   */
  private Map<ApplicationAttemptId,Map<RMContainer,Resource>> getContainersToPreempt(
      List<TempQueue> queues, Resource clusterResource) {

    Map<ApplicationAttemptId, Map<RMContainer,Resource>> preemptMap =
        new HashMap<ApplicationAttemptId, Map<RMContainer,Resource>>();
    
    List<RMContainer> skippedAMContainerlist = new ArrayList<RMContainer>();
    
    //for test only
    if(isTest){
    	
    	getContainersToPreemptForTest(preemptMap, queues, clusterResource);
    }
    

    for (TempQueue qT : queues) {
      if (qT.preemptionDisabled && qT.leafQueue != null) {
        if (LOG.isDebugEnabled()) {
          if (Resources.greaterThan(rc, clusterResource,
              qT.toBePreempted, Resource.newInstance(0, 0))) {
            LOG.info("Tried to preempt the following "
                      + "resources from non-preemptable queue: "
                      + qT.queueName + " - Resources: " + qT.toBePreempted);
          }
        }
        continue;
      }
      // we act only if we are violating balance by more than
      // maxIgnoredOverCapacity
      if (Resources.greaterThan(rc, clusterResource, qT.current,
          Resources.multiply(qT.guaranteed, 1.0 + maxIgnoredOverCapacity))) {
        // we introduce a dampening factor naturalTerminationFactor that
        // accounts for natural termination of containers
        Resource resToObtain =
          Resources.multiply(qT.toBePreempted, naturalTerminationFactor);
        Resource skippedAMSize = Resource.newInstance(0, 0);
        
        LOG.info("try to preempt: "+resToObtain+" from queue: "+qT.queueName);
        if(resToObtain.getMemory() > 0){
        	LOG.info("resToObtain memory: "+resToObtain.getMemory());
        }
        // lock the leafqueue while we scan applications and unreserve
        synchronized (qT.leafQueue) {
          //what is the descending order
          NavigableSet<FiCaSchedulerApp> ns = 
              (NavigableSet<FiCaSchedulerApp>) qT.leafQueue.getApplications();
         Iterator<FiCaSchedulerApp> desc = ns.descendingIterator();
          qT.actuallyPreempted = Resources.clone(resToObtain);
          while (desc.hasNext()) {
            FiCaSchedulerApp fc = desc.next();
            if (Resources.lessThanOrEqual(rc, clusterResource, resToObtain,
                Resources.none())) {
              break;
            }
            LOG.info("now try to preempt applicatin:"+fc.getApplicationId());
            preemptMap.put(
                fc.getApplicationAttemptId(),
                preemptFrom(fc, clusterResource, resToObtain,
                    skippedAMContainerlist, skippedAMSize));
          }
          
          //we allow preempt AM for kill based approach
          if(false){
         //we will never preempt am resource 
          Resource maxAMCapacityForThisQueue = Resources.multiply(
             Resources.multiply(clusterResource,
                  qT.leafQueue.getAbsoluteCapacity()),
              qT.leafQueue.getMaxAMResourcePerQueuePercent());

           //Can try preempting AMContainers (still saving atmost
          // maxAMCapacityForThisQueue AMResource's) if more resources are
          // required to be preempted from this Queue.
          preemptAMContainers(clusterResource, preemptMap,
             skippedAMContainerlist, resToObtain, skippedAMSize,
             maxAMCapacityForThisQueue);
          }
        }
      }
    }
    return preemptMap;
  }

  /**
   * As more resources are needed for preemption, saved AMContainers has to be
   * rescanned. Such AMContainers can be preempted based on resToObtain, but 
   * maxAMCapacityForThisQueue resources will be still retained.
   *  
   * @param clusterResource
   * @param preemptMap
   * @param skippedAMContainerlist
   * @param resToObtain
   * @param skippedAMSize
   * @param maxAMCapacityForThisQueue
   */
  private void preemptAMContainers(Resource clusterResource,
      Map<ApplicationAttemptId, Map<RMContainer,Resource>> preemptMap,
      List<RMContainer> skippedAMContainerlist, Resource resToObtain,
      Resource skippedAMSize, Resource maxAMCapacityForThisQueue) {
    for (RMContainer c : skippedAMContainerlist) {
      // Got required amount of resources for preemption, can stop now
      if (Resources.lessThanOrEqual(rc, clusterResource, resToObtain,
          Resources.none())) {
        break;
      }
      // Once skippedAMSize reaches down to maxAMCapacityForThisQueue,
      // container selection iteration for preemption will be stopped. 
      if (Resources.lessThanOrEqual(rc, clusterResource, skippedAMSize,
          maxAMCapacityForThisQueue)) {
        break;
      }
      Map<RMContainer,Resource> contToPrempt = preemptMap.get(c
          .getApplicationAttemptId());
      if (null == contToPrempt) {
        contToPrempt = new HashMap<RMContainer,Resource>();
        preemptMap.put(c.getApplicationAttemptId(), contToPrempt);
      }
      
      LOG.info("preempt am container "+c.getContainerId());
      contToPrempt.put(c,c.getContainer().getResource());
      
      Resources.subtractFrom(resToObtain, c.getContainer().getResource());
      Resources.subtractFrom(skippedAMSize, c.getContainer()
          .getResource());
    }
    skippedAMContainerlist.clear();
  }

  /**
   * Given a target preemption for a specific application, select containers
   * to preempt (after unreserving all reservation for that app).
   *
   * @param app
   * @param clusterResource
   * @param rsrcPreempt
   * @return Map<RMContainer,Resource> mapping from container to resource
   */
  private Map<RMContainer,Resource> preemptFrom(FiCaSchedulerApp app,
      Resource clusterResource, Resource rsrcPreempt,
      List<RMContainer> skippedAMContainerlist, Resource skippedAMSize) {
    Map<RMContainer,Resource> ret = new HashMap<RMContainer,Resource>();
    ApplicationAttemptId appId = app.getApplicationAttemptId();

    // first drop reserved containers towards rsrcPreempt
    List<RMContainer> reservations =
        new ArrayList<RMContainer>(app.getReservedContainers());
    for (RMContainer c : reservations) {
      if (Resources.lessThanOrEqual(rc, clusterResource,
          rsrcPreempt, Resources.none())) {
        return ret;
      }
      if (!observeOnly) {
        dispatcher.handle(new ContainerPreemptEvent(appId, c,
            ContainerPreemptEventType.DROP_RESERVATION, c.getContainer().getResource()));
      }
      Resources.subtractFrom(rsrcPreempt, c.getContainer().getResource());
    }

    // if more resources are to be freed go through all live containers in
    // reverse priority and reverse allocation order and mark them for
    // preemption
    List<RMContainer> containers =
      new ArrayList<RMContainer>(((FiCaSchedulerApp)app).getUnPreemtedContainers());

    sortContainersByResource(containers, clusterResource);

    for (RMContainer c : containers) {
      if (Resources.lessThanOrEqual(rc, clusterResource,
            rsrcPreempt, Resources.none())) {
        return ret;
      }
      
      // Skip AM Container from preemption for now.
      if (c.isAMContainer()) {
        skippedAMContainerlist.add(c);
        Resources.addTo(skippedAMSize, c.getContainer().getResource());
        continue;
      }
      // skip Labeled resource
      if(isLabeledContainer(c)){
        continue;
      }
     
      Resource preempteThisTime;
      if(isSuspended){
      //compute preempted resource this round
      //min(c.currentUsed,rsrcPreempt,c.SespendandResumeUnit)
      preempteThisTime  = Resources.mins(rc, clusterResource, rsrcPreempt,
    		              Resources.mins(rc, clusterResource, 
    		              c.getCurrentUsedResource(), c.getSRResourceUnit()));
      
      //if this container has all reousce preempted, continue
      if(Resources.equals(preempteThisTime,Resources.none())){
    	  continue;
      }
      
      }else{
       
      preempteThisTime = c.getContainer().getResource();
      
      }
      
      ret.put(c,preempteThisTime);
      LOG.info("get preempted Resource: "+preempteThisTime+" and container: "+c.getContainerId()+"current resource: "+c.getCurrentUsedResource());
      //substract preempted resource
      Resources.subtractFrom(rsrcPreempt, preempteThisTime);
    }

    return ret;
  }
  
  /**
   * Checking if given container is a labeled container
   * 
   * @param c
   * @return true/false
   */
  private boolean isLabeledContainer(RMContainer c) {
    return labels.containsKey(c.getAllocatedNode());
  }

  /**
   * Compare by reversed priority order first, and then reversed containerId
   * order
   * @param containers
   */
  @VisibleForTesting
  static void sortContainers(List<RMContainer> containers){
    Collections.sort(containers, new Comparator<RMContainer>() {
      @Override
      public int compare(RMContainer a, RMContainer b) {
        Comparator<Priority> c = new org.apache.hadoop.yarn.server
            .resourcemanager.resource.Priority.Comparator();
        int priorityComp = c.compare(b.getContainer().getPriority(),
                                     a.getContainer().getPriority());
        if (priorityComp != 0) {
          return priorityComp;
        }
        return b.getContainerId().compareTo(a.getContainerId());
      }
    });
  }
  
  @VisibleForTesting
  static void sortContainersByResource(List<RMContainer> containers, final Resource clusterResource){
    Collections.sort(containers, new Comparator<RMContainer>() {
      @Override
      public int compare(RMContainer a, RMContainer b) {
    	if(Resources.lessThan(rc, clusterResource, a.getCurrentUsedResource(), b.getCurrentUsedResource())){
    	   return 1;
    	      
    	}else{
    	  return -1;
       }
      }
    });
  }

  @Override
  public long getMonitoringInterval() {
    return monitoringInterval;
  }

  @Override
  public String getPolicyName() {
    return "ProportionalCapacityPreemptionPolicy";
  }


  /**
   * This method walks a tree of CSQueue and clones the portion of the state
   * relevant for preemption in TempQueue(s). It also maintains a pointer to
   * the leaves. Finally it aggregates pending resources in each queue and rolls
   * it up to higher levels.
   *
   * @param root the root of the CapacityScheduler queue hierarchy
   * @param clusterResources the total amount of resources in the cluster
   * @return the root of the cloned queue hierarchy
   */
  private TempQueue cloneQueues(CSQueue root, Resource clusterResources) {
    TempQueue ret;
    synchronized (root) {
      String queueName = root.getQueueName();
      float absUsed = root.getAbsoluteUsedCapacity();
      float absCap = root.getAbsoluteCapacity();
      float absMaxCap = root.getAbsoluteMaximumCapacity();
      boolean preemptionDisabled = root.getPreemptionDisabled();
      
      Resource current;
      
      //differenciate the resource based on if it is suspended
      if(isSuspended){
    	  current = root.getUsedResources();
      }else{
    	  current = Resources.multiply(clusterResources, absUsed);
      }
      
      float guaranteedRatio = absCap;
      Resource guaranteed   = Resources.multiply(clusterResources, absCap);
      Resource maxCapacity  = Resources.multiply(clusterResources, absMaxCap);
      
      Resource extra = Resource.newInstance(0, 0);
      if (Resources.greaterThan(rc, clusterResources, current, guaranteed)) {
        extra = Resources.subtract(current, guaranteed);
      }
      if (root instanceof LeafQueue) {
        LeafQueue l = (LeafQueue) root;
        Resource pending = l.getTotalResourcePending();
        
        LOG.info("cloneQueues current queue: "+queueName+" guarnateed:     "+guaranteed);
        LOG.info("cloneQueues current queue: "+queueName+" usedResource:   "+root.getUsedResources());
        LOG.info("cloneQueues current queue: "+queueName+" ratiodResource: "+Resources.multiply(clusterResources, absUsed));
        LOG.info("cloneQueues current queue: "+queueName+" pendingResource:"+pending);
        
        ret = new TempQueue(queueName, current, pending, guaranteed,
            maxCapacity, guaranteedRatio,preemptionDisabled);
        if (preemptionDisabled) {
          ret.untouchableExtra = extra;
        } else {
          ret.preemptableExtra = extra;
        }
        ret.setLeafQueue(l);
      } else {
        Resource pending = Resource.newInstance(0, 0);
        ret = new TempQueue(root.getQueueName(), current, pending, guaranteed,
            maxCapacity, guaranteedRatio,false);
        Resource childrensPreemptable = Resource.newInstance(0, 0);
        for (CSQueue c : root.getChildQueues()) {
          TempQueue subq = cloneQueues(c, clusterResources);
          Resources.addTo(childrensPreemptable, subq.preemptableExtra);
          ret.addChild(subq);
        }
        // untouchableExtra = max(extra - childrenPreemptable, 0)
        if (Resources.greaterThanOrEqual(
              rc, clusterResources, childrensPreemptable, extra)) {
          //it means there are some extra resource in child node are marked untouchable.
          ret.untouchableExtra = Resource.newInstance(0, 0);
        } else {
          ret.untouchableExtra =
                Resources.subtractFrom(extra, childrensPreemptable);
        }
      }
    }
    return ret;
  }

  // simple printout function that reports internal queue state (useful for
  // plotting)
  private void logToCSV(List<TempQueue> unorderedqueues){
    List<TempQueue> queues = new ArrayList<TempQueue>(unorderedqueues);
    Collections.sort(queues, new Comparator<TempQueue>(){
      @Override
      public int compare(TempQueue o1, TempQueue o2) {
        return o1.queueName.compareTo(o2.queueName);
      }});
    String queueState = " QUEUESTATE: " + clock.getTime();
    StringBuilder sb = new StringBuilder();
    sb.append(queueState);
    for (TempQueue tq : queues) {
      sb.append(", ");
      tq.appendLogString(sb);
    }
    LOG.info(sb.toString());
  }

  /**
   * Temporary data-structure tracking resource availability, pending resource
   * need, current utilization. Used to clone {@link CSQueue}.
   */
  static class TempQueue {
    final String queueName;
    final Resource current;
    final Resource pending;
    final Resource guaranteed;
    final Resource maxCapacity;
    Resource idealAssigned;
    Resource toBePreempted;
    Resource actuallyPreempted;
    Resource untouchableExtra;
    Resource preemptableExtra;
    
    float guaranteedRatio;

    double normalizedGuarantee;
    int dominantResource;

    final ArrayList<TempQueue> children;
    LeafQueue leafQueue;
    boolean preemptionDisabled;

    TempQueue(String queueName, Resource current, Resource pending,
        Resource guaranteed, Resource maxCapacity, float guaranteedRatio,boolean preemptionDisabled) {
      this.queueName = queueName;
      this.current = current;
      this.pending = pending;
      this.guaranteed = guaranteed;
      this.maxCapacity = maxCapacity;
      this.idealAssigned = Resource.newInstance(0, 0);
      this.actuallyPreempted = Resource.newInstance(0, 0);
      this.toBePreempted = Resource.newInstance(0, 0);
      this.normalizedGuarantee = Float.NaN;
      this.children = new ArrayList<TempQueue>();
      //both leaf and parent node may have untouchable resource
      this.untouchableExtra = Resource.newInstance(0, 0);
      //only leaf node has preemptable extra
      this.preemptableExtra = Resource.newInstance(0, 0);
      this.guaranteedRatio  = guaranteedRatio;
      this.preemptionDisabled = preemptionDisabled;
    }

    public void setLeafQueue(LeafQueue l){
      assert children.size() == 0;
      this.leafQueue = l;
    }

    /**
     * When adding a child we also aggregate its pending resource needs.
     * @param q the child queue to add to this queue
     */
    public void addChild(TempQueue q) {
      assert leafQueue == null;
      children.add(q);
      Resources.addTo(pending, q.pending);
    }

    public void addChildren(ArrayList<TempQueue> queues) {
      assert leafQueue == null;
      children.addAll(queues);
    }


    public ArrayList<TempQueue> getChildren(){
      return children;
    }

    // This function "accepts" all the resources it can (pending) and return
    // the unused ones
    Resource offer(Resource avail, ResourceCalculator rc,
        Resource clusterResource) {
 	
      Resource absMaxCapIdealAssignedDelta = Resources.componentwiseMax(
                      Resources.subtract(maxCapacity, idealAssigned),
                      Resource.newInstance(0, 0));
      
      // remain = avail - min(avail, (max - assigned), (current + pending - assigned))
      // we have bug here. in some case:
      //(current + pending - assigned).core > avail.core
      //(current + pending - assigned).memo < avail.memo
      //so we get least cores of the three and least memory of the three
      Resource possibleAccepted = 
          Resources.mins(rc, clusterResource, 
              absMaxCapIdealAssignedDelta,
          Resources.mins(rc, clusterResource, avail, Resources.subtract(
              Resources.add(current, pending), idealAssigned)));
      
      //final allocation resource
      Resource finalAccepted = Resources.clone(possibleAccepted);
      //in extrame case where avail cores are more less than the available memory, it may preempt mroe memory
      //Max:      1310720   320
      //avail:    542634    26
      //Delta:    734280    60  
      //Pending:  525312    120
      //current:  576512    260
      //ideal:    576512    260
      //then the accepted will be (525312,26) in which the memory is far more beyond the requirement
      
      if(isSuspended){
    	  
      if(dominantResource == Resources.CPU && !Resources.equals(pending,Resources.none())){
    	  //pending must be either none() or resource(int ,int)
    	  if(avail.getVirtualCores() == 0){
    	      //if the dominant resource is cpu, we will stop allocation even we have memory
    		  finalAccepted.setMemory(0);
    		  //but if we still have more available memory, we can allocate, to avoid preemption
    		  //we set memory to current usage
    		  int gapMemory = current.getMemory() - idealAssigned.getMemory();
    		  if(gapMemory > 0 && possibleAccepted.getMemory() > gapMemory){
    			  finalAccepted.setMemory(gapMemory);
    			  LOG.info("gap memory: "+gapMemory);
    		  }
    		  
    	  }else{
    	  double memoryRatio   = pending.getMemory()*1.0/pending.getVirtualCores();
    	  int    ratioedMemory = (int)(memoryRatio*possibleAccepted.getVirtualCores());
    	  finalAccepted.setMemory(ratioedMemory < possibleAccepted.getMemory() ? 
    			                   ratioedMemory:possibleAccepted.getMemory());
    	  }
    	  LOG.info("queue: "+queueName+" cpu dominant ");
    	  
    	  if(finalAccepted.getMemory() < possibleAccepted.getMemory()){
    		  LOG.info("previous memory: "+possibleAccepted.getMemory()+"  final memory: "+finalAccepted.getMemory());
    	  }
    	  
    	  
      }else if(dominantResource == Resources.MEMORY && !Resources.equals(pending, Resources.none())){
    	  if(avail.getMemory() == 0){
    		  finalAccepted.setVirtualCores(0);
    		  int gapCores = current.getVirtualCores() - idealAssigned.getVirtualCores();
    		  if(gapCores > 0 && possibleAccepted.getVirtualCores() > gapCores){
    			  finalAccepted.setVirtualCores(gapCores);
    			  LOG.info("gap cores: "+gapCores);
    		  }
    		  
    	  }else{
    	  double cpuRatio   = pending.getVirtualCores()*1.0/pending.getMemory();
    	  int    ratioedcpu = (int)(cpuRatio*possibleAccepted.getMemory());
    	  finalAccepted.setVirtualCores(ratioedcpu < possibleAccepted.getMemory() ? 
    			                 ratioedcpu:possibleAccepted.getMemory());
    	  }
    	  LOG.info("queue: "+queueName+" memory dominant ");
      }else{
    	  LOG.info("queue: "+queueName+" empty ");
      }
      
      }
 
      LOG.info("queueName:   "+queueName);
      LOG.info("beforeideal: "+idealAssigned);  
      Resource remain = Resources.subtract(avail, finalAccepted);
      Resources.addTo(idealAssigned, finalAccepted);
      LOG.info("avaul:       "+avail);
      LOG.info("absMaxDelta: "+absMaxCapIdealAssignedDelta);
      LOG.info("max:         "+maxCapacity);
      LOG.info("current:     "+current);
      LOG.info("pending:     "+pending);
      LOG.info("acceped:     "+finalAccepted);
      LOG.info("ideal:       "+idealAssigned);
      
      return remain;
      
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(" NAME: " + queueName)
        .append(" CUR: ").append(current)
        .append(" PEN: ").append(pending)
        .append(" GAR: ").append(guaranteed)
        .append(" NORM: ").append(normalizedGuarantee)
        .append(" IDEAL_ASSIGNED: ").append(idealAssigned)
        .append(" IDEAL_PREEMPT: ").append(toBePreempted)
        .append(" ACTUAL_PREEMPT: ").append(actuallyPreempted)
        .append(" UNTOUCHABLE: ").append(untouchableExtra)
        .append(" PREEMPTABLE: ").append(preemptableExtra)
        .append("\n");

      return sb.toString();
    }

    public void printAll() {
      LOG.info(this.toString());
      for (TempQueue sub : this.getChildren()) {
        sub.printAll();
      }
    }

    public void assignPreemption(float scalingFactor,
        ResourceCalculator rc, Resource clusterResource) {
      if (Resources.greaterThan(rc, clusterResource, current, idealAssigned)) {
    	  //to avoid negative memory or cores
    	  //we only prempte current.cores > idealAssigned.cores || current.memory > idealAssigned.memory
          toBePreempted = Resources.multiply(
              Resources.subtracts(current, idealAssigned), scalingFactor);
          //we still have resource left
          LOG.info("assignPreemption queue  "+queueName+" toBePreempted  "+toBePreempted);
      } else {
        toBePreempted = Resource.newInstance(0, 0);
      }
    }

    void appendLogString(StringBuilder sb) {
      sb.append("queuename").append(queueName).append(", ")
        .append("current memory").append(current.getMemory()).append(", ")
        .append("current core").append(current.getVirtualCores()).append(", ")
        .append("pending memory").append(pending.getMemory()).append(", ")
        .append("pending cores").append(pending.getVirtualCores()).append(", ")
        .append("guarante memory").append(guaranteed.getMemory()).append(", ")
        .append("guarante cores").append(guaranteed.getVirtualCores()).append(", ")
        .append("idealized memory").append(idealAssigned.getMemory()).append(", ")
        .append("idealized cores").append(idealAssigned.getVirtualCores()).append(", ")
        .append("preempt memory").append(toBePreempted.getMemory()).append(", ")
        .append("preempt cores").append(toBePreempted.getVirtualCores() ).append(", ")
        .append("actual memory").append(actuallyPreempted.getMemory()).append(", ")
        .append("actual cores").append(actuallyPreempted.getVirtualCores());
    }

  }

  static class TQComparator implements Comparator<TempQueue> {
    private ResourceCalculator rc;
    private Resource clusterRes;

    TQComparator(ResourceCalculator rc, Resource clusterRes) {
      this.rc = rc;
      this.clusterRes = clusterRes;
    }

    @Override
    public int compare(TempQueue tq1, TempQueue tq2) {
      if (getIdealPctOfGuaranteed(tq1) < getIdealPctOfGuaranteed(tq2)) {
        return -1;
      }
      if (getIdealPctOfGuaranteed(tq1) > getIdealPctOfGuaranteed(tq2)) {
        return 1;
      }
      return 0;
    }

    // Calculates idealAssigned / guaranteed
    // TempQueues with 0 guarantees are always considered the most over
    // capacity and therefore considered last for resources.
    private double getIdealPctOfGuaranteed(TempQueue q) {
      double pctOver = Integer.MAX_VALUE;
      if (q != null && Resources.greaterThan(
          rc, clusterRes, q.guaranteed, Resources.none())) {
        pctOver =
            Resources.divide(rc, clusterRes, q.idealAssigned, q.guaranteed);
      }
      return (pctOver);
    }
  }

}
