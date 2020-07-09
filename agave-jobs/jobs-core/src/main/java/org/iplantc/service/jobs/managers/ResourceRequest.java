package org.iplantc.service.jobs.managers;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.common.util.TimeUtils;
import org.iplantc.service.jobs.exceptions.JobProcessingException;
import org.iplantc.service.systems.model.BatchQueue;

/**
 * Internal class to match {@link BatchQueue} with the resource requests present in the job request.
 * This class also handles sanitization of the values in the resource request to proper numerical and temporal
 * value.
 */
public class ResourceRequest implements Comparable<BatchQueue> {
    private final Software software;
    private Long maxNodes;
    private Double maxMemoryPerNode;
    private Long maxProcessorsPerNode;
    private String maxRequestedTime;
    
    public ResourceRequest(Software software) {
        this.software = software;
    }

    /**
     * @return the number of nodes requested for the job
     */
    public Long getMaxNodes() {
        return maxNodes;
    }

    /**
     * Parses {@code maxNode} string value to Long and validates the value. If no value is provided, the default
     * {@link Software#getDefaultNodes()} will be used. If that is not specified, it defaults to 1.
     *
     * @param maxNodes the stringified max node value
     * @throws JobProcessingException if the values is not an integer value or is less than zero
     */
    public void setMaxNodes(String maxNodes) throws JobProcessingException {
        if (StringUtils.isBlank(maxNodes))
        {
            // use the software default queue if present
            if (getSoftware().getDefaultNodes() != null && getSoftware().getDefaultNodes() != -1) {
                this.maxNodes = getSoftware().getDefaultNodes();
            } else {
                this.maxNodes = 1L;
            }
        }
        else
        {
            this.maxNodes = NumberUtils.toLong(maxNodes);
        }

        if (this.maxNodes != null && this.maxNodes <= 0)
        {
            throw new JobProcessingException(400,
                    "Invalid " + (StringUtils.isEmpty(maxNodes) ? "" : "default ") +
                            "nodeCount. If specified, nodeCount must be a positive integer value.");
        }
    }

    /**
     * @return the memory per node, in GB, requested for the job
     */
    public Double getMaxMemoryPerNode() {
        return maxMemoryPerNode;
    }

    /**
     * Parses {@code maxMemoryPerNode} string value to Double value in GB. Valid formats are double values and strings in
     * ###.#[EPTGM]B format. If no value is provided, the default {@link Software#getDefaultMemoryPerNode()} will be
     * used. If that is not specified, an exception will be thrown.
     *
     * @param maxMemoryPerNode the max memory per node in ###.#[EPTGM]B format. If no magnitude is specified, GB is assumed.
     * @throws JobProcessingException if the value does not parse, or is less than zero
     */
    public void setMaxMemoryPerNode(String maxMemoryPerNode) throws JobProcessingException {
        if (StringUtils.isBlank(maxMemoryPerNode))
        {
            if (getSoftware().getDefaultMemoryPerNode() != null) {
                this.maxMemoryPerNode = getSoftware().getDefaultMemoryPerNode();
            }
        }
        else // memory was given, validate
        {
            try {
                // try to parse it as a number in GB first
                this.maxMemoryPerNode = Double.parseDouble(maxMemoryPerNode);
            }
            catch (Throwable e)
            {
                // Otherwise parse it as a string matching ###.#[EPTGM]B
                try
                {
                    this.maxMemoryPerNode = BatchQueue.parseMaxMemoryPerNode(maxMemoryPerNode);
                }
                catch (NullPointerException|NumberFormatException e1)
                {
                    this.maxMemoryPerNode = 0.0D;
                }
            }
        }

        if (this.maxMemoryPerNode != null && this.maxMemoryPerNode <= 0D) {
            throw new JobProcessingException(400,
                    "Invalid " + (StringUtils.isEmpty(maxMemoryPerNode) ? "" : "default ") +
                            "memoryPerNode. memoryPerNode should be a postive value specified in ###.#[EPTGM]B format.");
        }
    }

    /**
     * @return the max processors per node requested for the job
     */
    public Long getMaxProcessorsPerNode() {
        return maxProcessorsPerNode;
    }

    /**
     * Parses {@code maxProcessorsPerNode} string value to Long and validates the value. If no value is provided, the
     * {@link Software#getDefaultProcessorsPerNode()} will be used. If that is not specified, it defaults to 1.
     *
     * @param maxProcessorsPerNode the stringified max processors per node value
     * @throws JobProcessingException if the values is not an integer value or is less than zero
     */
    public void setMaxProcessorsPerNode(String maxProcessorsPerNode) throws JobProcessingException {
        if (StringUtils.isBlank(maxProcessorsPerNode))
        {
            if (getSoftware().getDefaultProcessorsPerNode() != null) {
                this.maxProcessorsPerNode = getSoftware().getDefaultProcessorsPerNode();
            } else {
                this.maxProcessorsPerNode = 1L;
            }
        }
        else
        {
            this.maxProcessorsPerNode = NumberUtils.toLong(maxProcessorsPerNode);
        }

        if (this.maxProcessorsPerNode != null && this.maxProcessorsPerNode <= 0) {
            throw new JobProcessingException(400,
                    "Invalid " + (StringUtils.isEmpty(maxProcessorsPerNode) ? "" : "default ") +
                            "processorsPerNode value. processorsPerNode must be a positive integer value.");
        }
    }

    /**
     * @return the max time requested for the job in ##:##:## format
     */
    public String getMaxRequestedTime() {
        return maxRequestedTime;
    }

    /**
     * Validates {@code maxRequestedTime} value. If no value is provided, the {@link Software#getDefaultMaxRunTime()}
     * will be used. If that is not specified, it defaults to the
     *
     * @param maxRequestedTime the max run time in ##:##:## format
     * @throws JobProcessingException if the values is not an integer value or is less than zero
     */
    public void setMaxRequestedTime(String maxRequestedTime) throws JobProcessingException {
        if (StringUtils.isBlank(maxRequestedTime))
        {
            if (!StringUtils.isEmpty(getSoftware().getDefaultMaxRunTime())) {
                this.maxRequestedTime = getSoftware().getDefaultMaxRunTime();
            }
        }
        else
        {
            this.maxRequestedTime = maxRequestedTime;
        }

        if (this.maxRequestedTime != null && !TimeUtils.isValidRequestedJobTime(this.maxRequestedTime)) {
            throw new JobProcessingException(400,
                    "Invalid maxRunTime. maxRunTime should be the maximum run " +
                            "time for this job in hh:mm:ss format.");
//			} else if (TimeUtils.compareRequestedJobTimes(this.maxRequestedTime, "00:00:00") <= 0) {
//				throw new JobProcessingException(400,
//						"Invalid maxRunTime. maxRunTime should be greater than 00:00:00.");
        }
    }

    /**
     * Comparison method so we can compare a resource request with a {@link BatchQueue} as a simple validation
     * mechanism. If this {@link #compareTo(BatchQueue)} evaluates to 1, the request cannot be satisfied by the
     * queue. If it evaluates to anything else, the {@link BatchQueue} will satisfy the request.
     * @param batchQueue the queue to compare against the {@link ResourceRequest}
     * @return 1 if the queue can satisfy the resource request, 0 if it exactly fits, -1 if it easily fits.
     */
    public int compareTo(BatchQueue batchQueue) {
        if (batchQueue == null) return 1;

        // if either is null, or the queue supports max nodes, then it passes
        int nodeComp = NumberUtils.compare((maxNodes == null ? 0 : maxNodes), (batchQueue.getMaxNodes() == -1 ? Long.MAX_VALUE : batchQueue.getMaxNodes()));
        int memComp = NumberUtils.compare((maxMemoryPerNode == null ? 0 : maxMemoryPerNode), (batchQueue.getMaxMemoryPerNode() == -1 ? Double.MAX_VALUE : batchQueue.getMaxMemoryPerNode()));
        int procComp = NumberUtils.compare((maxProcessorsPerNode == null ? 0 : maxProcessorsPerNode), (batchQueue.getMaxProcessorsPerNode() == -1 ? Long.MAX_VALUE : batchQueue.getMaxProcessorsPerNode()));
        int timeComp = TimeUtils.compareRequestedJobTimes(maxRequestedTime == null ? BatchQueue.DEFAULT_MIN_RUN_TIME : maxRequestedTime, batchQueue.getMaxRequestedTime());

        // if the request and queue match on all values, then return a match
        if (nodeComp == 0 && memComp == 0 && procComp == 0 && timeComp == 0) {
            return 0;
        // if any request value exceeded the corresponding queue value, then no match, the request is larger
        } else if (nodeComp > 0 || memComp > 0 || procComp > 0 || timeComp > 0) {
            return 1;
        // if the queue equals or exceeds the request on every corresponding value, then return a match
        } else {
            return -1;
        }

//        }
//        if (maxNodes == null || batchQueue.getMaxNodes() == -1) {
//            if (maxMemoryPerNode == null || batchQueue.getMaxMemoryPerNode() == -1) {
//                if (maxProcessorsPerNode == null || batchQueue.getMaxProcessorsPerNode() == -1) {
//                    return TimeUtils.compareRequestedJobTimes(maxRequestedTime == null ? BatchQueue.DEFAULT_MIN_RUN_TIME : maxRequestedTime, batchQueue.getMaxRequestedTime());
//                } else {
//                    return NumberUtils.compare((maxProcessorsPerNode == null ? 0 : maxProcessorsPerNode), (batchQueue.getMaxProcessorsPerNode() == -1 ? Long.MAX_VALUE : batchQueue.getMaxProcessorsPerNode()));
//                }
//            } else {
//                return NumberUtils.compare((maxMemoryPerNode == null ? 0 : maxMemoryPerNode), (batchQueue.getMaxMemoryPerNode() == -1 ? Double.MAX_VALUE : batchQueue.getMaxMemoryPerNode()));
//            }
//        } else {
//            return NumberUtils.compare((maxNodes == null ? 0 : maxNodes), (batchQueue.getMaxNodes() == -1 ? Long.MAX_VALUE : batchQueue.getMaxNodes()));
//        }
    }

    public Software getSoftware() {
        return software;
    }

    /**
     * @param maxNodes the max nodes requested
     */
    public void setMaxNodes(Long maxNodes) {
        this.maxNodes = maxNodes;
    }

    /**
     * @param maxMemoryPerNode the max memory per node requested
     */
    public void setMaxMemoryPerNode(Double maxMemoryPerNode) {
        this.maxMemoryPerNode = maxMemoryPerNode;
    }

    /**
     * @param maxProcessorsPerNode the max processors per nodes requested
     */
    public void setMaxProcessorsPerNode(Long maxProcessorsPerNode) {
        this.maxProcessorsPerNode = maxProcessorsPerNode;
    }
}
