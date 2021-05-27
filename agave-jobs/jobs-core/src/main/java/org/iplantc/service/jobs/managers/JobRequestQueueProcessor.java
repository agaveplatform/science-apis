package org.iplantc.service.jobs.managers;

import org.apache.commons.lang.StringUtils;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.common.util.TimeUtils;
import org.iplantc.service.jobs.exceptions.JobProcessingException;
import org.iplantc.service.jobs.exceptions.NoMatchingBatchQueueException;
import org.iplantc.service.systems.model.BatchQueue;
import org.iplantc.service.systems.model.ExecutionSystem;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

/**
 * Handles validation of queue selection and 
 * limits in a job request.
 * @author dooley
 *
 */
public class JobRequestQueueProcessor {

	final private ExecutionSystem executionSystem;
	final private Software software;
	final private String username;

	final private ResourceRequest resourceRequest;

	private BatchQueue userBatchQueue;
	private BatchQueue softwareBatchQueue;
	private BatchQueue matchingBatchQueue;

	/**
	 * Instantiates a new JobRequestQueueProcessor which will perform the matchmaking and validation of the proper
	 * queue to which the job will be submitted on the target system.
	 *
	 * @param software the app to be run by the job request
	 * @param executionSystem the system on which the job should be run
	 * @param username the user submitting the job requet
	 */
	public JobRequestQueueProcessor(Software software, ExecutionSystem executionSystem, String username) {
		this.executionSystem = executionSystem;
		this.software = software;
		this.username = username;
		this.resourceRequest = new ResourceRequest(software);
	}
	
	/**
	 * Validates the resource request value provided in the job requets and attempts to select a matching queue. The
	 * queue selection algorithm is as follows.
	 * <ol>
	 * <li>If the user supplies a batch queue name in the job request, that is used.</li>
	 * <li>If the software as specified in the job request has a default queue, that is used.</li>
	 * <li>If the execution system has a default queue, that matches the resources requested, that is used.</li>
	 * <li>The first batch queue on the execution system that satisfies the resources requested is used.</li>
	 * </ol>
	 *
	 * Once a {@link BatchQueue} is selected, it is validated against the resources requested and, if successful, set
	 * as the {@link #matchingBatchQueue}.
	 * 
	 * @param jobRequestMap  the job requet map from which the queue info should be processed
	 * @throws JobProcessingException if a resource constraint is invalid
	 * @throws NoMatchingBatchQueueException if no queue satisfying the constraint can be found.
	 */
	public void process(Map<String, Object> jobRequestMap) throws JobProcessingException, NoMatchingBatchQueueException
	{
		BatchQueue selectedQueue = null;

		// parses all the queue and resource requet info from the job request into ResourceRequest, userBatchQueue
		// and softwareBatchQueue objects
		parseResourceRequestFromJobRequest(jobRequestMap);


		if (getUserBatchQueue() == null) {
			if (getSoftwareBatchQueue() == null) {
				selectedQueue = findMatching(getResourceRequest());
			} else {
				selectedQueue = getSoftwareBatchQueue();
			}
		} else {
			selectedQueue = getUserBatchQueue();
		}


		if (validateBatchSubmitParameters(selectedQueue, getResourceRequest()))  {
			setMatchingBatchQueue(selectedQueue);
		} else {
			throw new NoMatchingBatchQueueException(String.format("No queue found on execution system %s to support jobs " +
					"requiring %d nodes with %f memmory and %d processors per node, and a max run time of %s.",
					getExecutionSystem().getSystemId(),
					getResourceRequest().getMaxNodes(),
					getResourceRequest().getMaxMemoryPerNode(),
					getResourceRequest().getMaxProcessorsPerNode(),
					getResourceRequest().getMaxRequestedTime()));
		}
	}

	/**
	 * Parses out all resource and queue info from the request and populates the internal variables with the info.
	 * @param jobRequestMap job request map sent from the user
	 * @throws JobProcessingException if a resource reqeut parameter is invalid, or either of the queue names is invalid
	 */
	protected void parseResourceRequestFromJobRequest(Map<String, Object> jobRequestMap) throws JobProcessingException, NoMatchingBatchQueueException {

		// pull batch queue value from job requet
		String userBatchQueueName = (String)jobRequestMap.get("batchQueue");
		// if that's not supplied, check the legacy queue parameter
		if (StringUtils.isEmpty(userBatchQueueName)) {
			userBatchQueueName = (String)jobRequestMap.get("queue");
		}
		setUserBatchQueue(userBatchQueueName);

		// if a queue is defined by the Software, resolve that as well
		setSoftwareBatchQueue(getSoftware().getDefaultQueue());

		// pull nodecount value from job request
		String userNodeCount = (String)jobRequestMap.get("nodeCount");
		getResourceRequest().setMaxNodes(userNodeCount);

		// pull memory per node value from job request
		String userMemoryPerNode = (String)jobRequestMap.get("memoryPerNode");
		// fall back on legacy field name if the current one is not present
		if (StringUtils.isEmpty(userMemoryPerNode)) {
			userMemoryPerNode = (String)jobRequestMap.get("maxMemory");
		}
		try {
			getResourceRequest().setMaxMemoryPerNode(userMemoryPerNode);
		} catch (JobProcessingException e) {
			// use the max run time for the queue if no time was given by the user or system
			if (getUserBatchQueue() != null) {
				getResourceRequest().setMaxMemoryPerNode(getUserBatchQueue().getMaxMemoryPerNode());
			} else if (getSoftwareBatchQueue() != null) {
				getResourceRequest().setMaxMemoryPerNode(getSoftwareBatchQueue().getMaxMemoryPerNode());
			} else {
//				throw e;
			}
		}

		// pull max runtime value from job requet
		String userRequestedTime = (String)jobRequestMap.get("maxRunTime");
		// fall back on legacy field name if the current one is not present
		if (StringUtils.isEmpty(userRequestedTime)) {
			// legacy compatibility
			userRequestedTime = (String)jobRequestMap.get("requestedTime");
		}
		getResourceRequest().setMaxRequestedTime(userRequestedTime);

		// pull batch processors per node value from job request
		String userProcessorsPerNode = (String)jobRequestMap.get("processorsPerNode");
		// fall back on legacy field name if the current one is not present
		if (StringUtils.isEmpty(userProcessorsPerNode)) {
			userProcessorsPerNode = (String)jobRequestMap.get("processorCount");
		}
		getResourceRequest().setMaxProcessorsPerNode(userProcessorsPerNode);

	}

	/**
	 * Finds queue on the given executionSystem that supports the given number of nodes and
	 * memory per node given.
	 *
	 * @param nodes a positive integer value or -1 for no limit
	 * @param memory memory in GB or -1 for no limit
	 * @param requestedTime time in hh:mm:ss format
	 * @return a BatchQueue matching the given parameters or null if no match can be found
	 */
	private BatchQueue selectQueue(Long nodes, Double memory, String requestedTime) throws NoMatchingBatchQueueException
	{
		return selectQueue(nodes, memory, (long)-1, requestedTime);
	}

	/**
	 * Finds queue on the {@link #getExecutionSystem()} that supports the number of {@code nodes}, {@code processors},
	 * {@code memory}, and {@code requestedTime}.
	 *
	 * @param nodes a positive integer value or -1 for no limit
	 * @param processors positive integer value or -1 for no limit
	 * @param memory memory in GB or -1 for no limit
	 * @param requestedTime time in hh:mm:ss format
	 * @return a BatchQueue matching the given parameters or null if no match can be found
	 */
	private BatchQueue selectQueue(Long nodes, Double memory, Long processors, String requestedTime) throws NoMatchingBatchQueueException
	{
		if (validateBatchSubmitParameters(getExecutionSystem().getDefaultQueue(), nodes, processors, memory, requestedTime))
		{
			return getExecutionSystem().getDefaultQueue();
		}
		else
		{
			BatchQueue[] queues = getExecutionSystem().getBatchQueues().toArray(new BatchQueue[]{});
			Arrays.sort(queues);
			for (BatchQueue queue: queues)
			{
				if (queue.isSystemDefault())
					continue;
				else if (validateBatchSubmitParameters(queue, nodes, processors, memory, requestedTime))
					return queue;
			}
		}

		throw new NoMatchingBatchQueueException(String.format("No queue found on execution system %s to support jobs " +
				"requiring %d nodes with %f memmory and %d processors per node, and a max run time of %s.",
			getExecutionSystem().getSystemId(), nodes, memory, processors, requestedTime));
	}

	/**
	 * Finds queue on the {@link #getExecutionSystem()} that supports the {@code resourceRequest}.
	 *
	 * @param resourceRequest a positive integer value or -1 for no limit
	 * @return a BatchQueue matching the given parameters or null if no match can be found
	 * @throws NoMatchingBatchQueueException if no matching queue can be found.
	 */
	protected BatchQueue findMatching(ResourceRequest resourceRequest) throws NoMatchingBatchQueueException //(Long nodes, Double memory, Long processors, String requestedTime) throws NoMatchingBatchQueueException
	{
		// try the default queue first...because it's the default
		if (validateBatchSubmitParameters(getExecutionSystem().getDefaultQueue(), resourceRequest))
		{
			return getExecutionSystem().getDefaultQueue();
		}
		else
		{
			// filter out all queues that do not match
			Optional<BatchQueue> matchingQueue = getExecutionSystem().getBatchQueues()
					.stream()
					// sort by system owner ordering
					.sorted()
					// filter out any that do not match
					.filter(queue -> !queue.isSystemDefault() && validateBatchSubmitParameters(queue, resourceRequest))
					// return the first match. In the future we could look at some some sort of load balancing algorithm
					// to distribute jobs across queues, but for now, we'll go with first matching that is not full.
					.findFirst();

			if (matchingQueue.isPresent()) {
				return matchingQueue.get();
			} else {
				throw new NoMatchingBatchQueueException(String.format("No queue found on execution system %s to support jobs " +
						"requiring %d nodes with %f memmory and %d processors per node, and a max run time of %s.",
						getExecutionSystem().getSystemId(),
						resourceRequest.getMaxNodes(),
						resourceRequest.getMaxMemoryPerNode(),
						resourceRequest.getMaxProcessorsPerNode(),
						resourceRequest.getMaxRequestedTime()));
			}
		}
	}

//	/**
//	 * Fetches current {@link BatchQueueLoad} for the given {@code username} on the {@code batchQueueName}. This
//	 * represents instantaneous status in the db and will change from moment to moment.
//	 * @param batchQueueName name of the queue on the requested execution system.
//	 * @param username the job request owner
//	 * @return the current queue load attributed to the {@code username} on the {@code batchQueueName} or an empty {@link BatchQueueLoad} if unable to obtain
//	 * @deprecated used in a scoring function when matching and selecting queues dynamically per request
//	 */
//	private BatchQueueLoad getBatchQueueLoadForUser(String batchQueueName, String username) {
//		try {
//			BatchQueueLoad load = new BatchQueueDao().getCurrentLoadForUser(getExecutionSystem().getSystemId(), batchQueueName, username);
//			if (load != null) {
//				return load;
//			}
//		} catch (Exception ignored) {}
//
//		return new BatchQueueLoad(batchQueueName);
//	}
//
//	/**
//	 * Fetches current {@link BatchQueueLoad} on the {@code batchQueueName}. This
//	 * represents instantaneous status in the db and will change from moment to moment.
//	 * @param batchQueueName name of the queue on the requested execution system.
//	 * @return the current queue load on the {@code batchQueueName} or an empty {@link BatchQueueLoad} if unable to obtain
//	 * @deprecated used in a scoring function when matching and selecting queues dynamically per request
//	 */
//	private BatchQueueLoad getBatchQueueLoad(String batchQueueName) {
//		try {
//			BatchQueueLoad load = new BatchQueueDao().getCurrentLoad(getExecutionSystem().getSystemId(), batchQueueName);
//			if (load != null) {
//				return load;
//			}
//		} catch (Exception ignored) {}
//
//		return new BatchQueueLoad(batchQueueName);
//	}
//

	/**
	 * Validates that the queue supports the number of nodes, processors per node, memory and
	 * requestedTime provided. If any of these values are null or the given values exceed the queue
	 * limits, it returns false.
	 *
	 * @param queue the BatchQueue to check against
	 * @param nodes a positive integer value or -1 for no limit
	 * @param processors positive integer value or -1 for no limit
	 * @param memory memory in GB or -1 for no limit
	 * @param requestedTime time in hh:mm:ss format
	 * @return true if all the values are non-null and within the limits of the queue
	 */
	private boolean validateBatchSubmitParameters(BatchQueue queue, Long nodes, Long processors, Double memory, String requestedTime)
	{
		if (queue == null ||
			nodes == null ||  nodes == 0 || nodes < -1 ||
			processors == null || processors == 0 || processors < -1 ||
			memory == null || memory == 0 || memory < -1 ||
			StringUtils.isEmpty(requestedTime) || StringUtils.equals("00:00:00", requestedTime))
		{
			return false;
		}

		if (queue.getMaxNodes() > 0 && queue.getMaxNodes() < nodes) {
			return false;
		}

		if (queue.getMaxProcessorsPerNode() > 0 && queue.getMaxProcessorsPerNode() < processors) {
			return false;
		}

		if (queue.getMaxMemoryPerNode() > 0 && queue.getMaxMemoryPerNode() < memory) {
			return false;
		}

		return queue.getMaxRequestedTime() == null || TimeUtils.compareRequestedJobTimes(queue.getMaxRequestedTime(), requestedTime) != -1;
	}

	/**
	 * Validates that the queue supports the number of nodes, processors per node, memory and
	 * requestedTime provided. If any of these values are null or the given values exceed the queue
	 * limits, it returns false.
	 *
	 * @param queue the BatchQueue to check against
	 * @param resourceRequest a positive integer value or -1 for no limit
	  */
	protected boolean validateBatchSubmitParameters(BatchQueue queue, ResourceRequest resourceRequest)//Long nodes, Long processors, Double memory, String requestedTime)
	{
//		if (queue == null ||
//				resourceRequest.getMaxNodes() <= 0 ||
//				resourceRequest.getMaxProcessorsPerNode() <= 0 ||
//				resourceRequest.getMaxMemoryPerNode() <= 0 ||
//				StringUtils.isEmpty(resourceRequest.getMaxRequestedTime()) ||
//				StringUtils.equals("00:00:00", resourceRequest.getMaxRequestedTime()))
//		{
//			return false;
//		}

		return resourceRequest.compareTo(queue) <= 0;
//		if (queue.getMaxNodes() > 0 && queue.getMaxNodes() < nodes) {
//			return false;
//		}
//
//		if (queue.getMaxProcessorsPerNode() > 0 && queue.getMaxProcessorsPerNode() < processors) {
//			return false;
//		}
//
//		if (queue.getMaxMemoryPerNode() > 0 && queue.getMaxMemoryPerNode() < memory) {
//			return false;
//		}
//
//		if (queue.getMaxRequestedTime() != null && TimeUtils.compareRequestedJobTimes(queue.getMaxRequestedTime(), requestedTime) == -1) {
//			return false;
//		}
//
//		return true;
	}

	public ExecutionSystem getExecutionSystem() {
		return executionSystem;
	}

	public Software getSoftware() {
		return software;
	}

	public ResourceRequest getResourceRequest() {
		return resourceRequest;
	}

	/**
	 * The user-specified queue from the job requet
	 * @return the named queue from the job request or null if not specified
	 */
	public BatchQueue getUserBatchQueue() {
		return userBatchQueue;
	}

	/**
	 * @param userBatchQueue the queue to set
	 */
	protected void setUserBatchQueue(BatchQueue userBatchQueue) {
		this.userBatchQueue = userBatchQueue;
	}

	/**
	 * Assigns the batch queue for the given {@link #getExecutionSystem()} as requested by the user.
	 * @param userBatchQueueName the name of the defaultQueue in the job request
	 * @throws NoMatchingBatchQueueException if the named queue does not exist on the given system
	 */
	protected void setUserBatchQueue(String userBatchQueueName) throws NoMatchingBatchQueueException {
		if (StringUtils.isNotEmpty(userBatchQueueName)) {
			BatchQueue userQueue = getExecutionSystem().getQueue(userBatchQueueName);
			if (userQueue == null) {
				throw new NoMatchingBatchQueueException(
						"Invalid batchQueue. No batch queue named " + userBatchQueueName +
								" is defined on system " + getExecutionSystem().getSystemId());
			} else {
				setUserBatchQueue(userQueue);
			}
		}
	}

	/**
	 * The {@link BatchQueue} named by the {@link Software#getDefaultQueue()}
	 * @return the named queue for the {@link Software} named in the job requet, or null if not specified
	 */
	public BatchQueue getSoftwareBatchQueue() {
		return softwareBatchQueue;
	}

	protected void setSoftwareBatchQueue(BatchQueue softwareBatchQueue) {
		this.softwareBatchQueue = softwareBatchQueue;
	}

	/**
	 * Assigns the batch queue based on the queue name for the given {@link #getExecutionSystem()}.
	 * @param softwareBatchQueueName  the name of the default queue defined for the {@code Software} in the job request
	 * @throws NoMatchingBatchQueueException if the named queue does not exist on the given system
	 */
	protected void setSoftwareBatchQueue(String softwareBatchQueueName) throws NoMatchingBatchQueueException {
		if (StringUtils.isNotEmpty(softwareBatchQueueName)) {
			BatchQueue softwareQueue = getExecutionSystem().getQueue(softwareBatchQueueName);
			if (softwareQueue == null) {
				throw new NoMatchingBatchQueueException(
						"No queue matching the default queue, \"" + getSoftware().getDefaultQueue() +
						",\" specified by the requested app " + getSoftware().getUniqueName() +
						", is not defined on the execution system " + getExecutionSystem().getSystemId());
			} else {
				this.softwareBatchQueue = softwareQueue;
			}
		}
	}

	public BatchQueue getMatchingBatchQueue() {
		return matchingBatchQueue;
	}

	protected void setMatchingBatchQueue(BatchQueue matchingBatchQueue) {
		this.matchingBatchQueue = matchingBatchQueue;
	}

	protected String getUsername() {
		return username;
	}
}
