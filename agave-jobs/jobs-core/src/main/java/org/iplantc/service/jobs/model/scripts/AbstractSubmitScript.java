package org.iplantc.service.jobs.model.scripts;

import org.iplantc.service.apps.model.Software;
import org.iplantc.service.apps.model.enumerations.ParallelismType;
import org.iplantc.service.jobs.exceptions.JobMacroResolutionException;
import org.iplantc.service.jobs.managers.launchers.WrapperTemplateMacroResolver;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.util.Slug;
import org.iplantc.service.systems.model.BatchQueue;
import org.iplantc.service.systems.model.ExecutionSystem;

public abstract class AbstractSubmitScript implements SubmitScript {

	protected String			name;
	protected boolean			inCurrentWorkingDirectory	= true;
	protected boolean			verbose						= true;
	protected String			standardOutputFile;
	protected String			standardErrorFile;
	protected String			time						= "01:00:00";
	protected ParallelismType	parallelismType				= ParallelismType.SERIAL;
	protected BatchQueue		queue;
	protected long				nodes						= 1;
	protected long				processors					= 1;
	protected double			memoryPerNode				= 1.0;
	protected String			batchInstructions;
	protected ExecutionSystem 	executionSystem;
	protected Software			software;
	protected Job				job;

	/**
	 * Default constructor used by all {@link SubmitScript}. Note that node count will be forced to 1
	 * whenever the {@link Software#getParallelism()} is {@link ParallelismType#SERIAL} or null.
	 *
	 * @param job the job for which the submit script is being created
	 * @param software the app being run by the job
	 * @param executionSystem the system on which the app will be run
	 */
	public AbstractSubmitScript(Job job, Software software, ExecutionSystem executionSystem)
	{
		this.job = job;
		this.executionSystem = executionSystem;
		this.software = software;
		setName(job.getName());
		this.standardOutputFile = name + "-" + job.getUuid() + ".out";
		this.standardErrorFile = name + "-" + job.getUuid() + ".err";
		
		this.nodes = job.getNodeCount();
		this.memoryPerNode = job.getMemoryPerNode();
		this.processors = job.getProcessorsPerNode();
		this.time = job.getMaxRunTime();
		setParallelismType(software.getParallelism());
		this.queue = this.executionSystem.getQueue(job.getBatchQueue());
	}
	
	/* (non-Javadoc)
	 * @see org.iplantc.service.jobs.model.scripts.SubScript#getName()
	 */
	@Override
	public String getName()
	{
		return name;
	}

	/* (non-Javadoc)
	 * @see org.iplantc.service.jobs.model.scripts.SubScript#setName(java.lang.String)
	 */
	@Override
	public void setName(String name)
	{
		name = Slug.toSlug(name);
		
		if (Character.isDigit(name.charAt(0))) {
			name = "agave-" + name;
		}
		
		this.name = name;
	}
	
	static void main (String[] args) {
		
	}

	/* (non-Javadoc)
	 * @see org.iplantc.service.jobs.model.scripts.SubScript#isInCurrentWorkingDirectory()
	 */
	@Override
	public boolean isInCurrentWorkingDirectory()
	{
		return inCurrentWorkingDirectory;
	}

	/* (non-Javadoc)
	 * @see org.iplantc.service.jobs.model.scripts.SubScript#setInCurrentWorkingDirectory(boolean)
	 */
	@Override
	public void setInCurrentWorkingDirectory(boolean inCurrentWorkingDirectory)
	{
		this.inCurrentWorkingDirectory = inCurrentWorkingDirectory;
	}

	/* (non-Javadoc)
	 * @see org.iplantc.service.jobs.model.scripts.SubScript#isVerbose()
	 */
	@Override
	public boolean isVerbose()
	{
		return verbose;
	}

	/* (non-Javadoc)
	 * @see org.iplantc.service.jobs.model.scripts.SubScript#setVerbose(boolean)
	 */
	@Override
	public void setVerbose(boolean verbose)
	{
		this.verbose = verbose;
	}

	/* (non-Javadoc)
	 * @see org.iplantc.service.jobs.model.scripts.SubScript#getStandardOutputFile()
	 */
	@Override
	public String getStandardOutputFile()
	{
		return standardOutputFile;
	}

	/* (non-Javadoc)
	 * @see org.iplantc.service.jobs.model.scripts.SubScript#setStandardOutputFile(java.lang.String)
	 */
	@Override
	public void setStandardOutputFile(String standardOutputFile)
	{
		this.standardOutputFile = standardOutputFile;
	}

	/* (non-Javadoc)
	 * @see org.iplantc.service.jobs.model.scripts.SubScript#getStandardErrorFile()
	 */
	@Override
	public String getStandardErrorFile()
	{
		return standardErrorFile;
	}

	/* (non-Javadoc)
	 * @see org.iplantc.service.jobs.model.scripts.SubScript#setStandardErrorFile(java.lang.String)
	 */
	@Override
	public void setStandardErrorFile(String standardErrorFile)
	{
		this.standardErrorFile = standardErrorFile;
	}

	/* (non-Javadoc)
	 * @see org.iplantc.service.jobs.model.scripts.SubScript#getTime()
	 */
	@Override
	public String getTime()
	{
		return time;
	}

	/* (non-Javadoc)
	 * @see org.iplantc.service.jobs.model.scripts.SubScript#setTime(java.lang.String)
	 */
	@Override
	public void setTime(String time)
	{
		this.time = time;
	}

	/* (non-Javadoc)
	 * @see org.iplantc.service.jobs.model.scripts.SubScript#getParallelismType()
	 */
	@Override
	public ParallelismType getParallelismType()
	{
		return parallelismType;
	}

	/* (non-Javadoc)
	 * @see org.iplantc.service.jobs.model.scripts.SubScript#setParallelismType(org.iplantc.service.apps.model.enumerations.ParallelismType)
	 */
	@Override
	public void setParallelismType(ParallelismType parallelismType)
	{
		this.parallelismType = parallelismType == null ? ParallelismType.SERIAL : parallelismType;
		if (parallelismType == ParallelismType.SERIAL) {
			this.nodes = 1;
		}
	}

	/* (non-Javadoc)
	 * @see org.iplantc.service.jobs.model.scripts.SubScript#getProcessors()
	 */
	@Override
	public long getProcessors()
	{
		return processors;
	}

	/* (non-Javadoc)
	 * @see org.iplantc.service.jobs.model.scripts.SubScript#setProcessors(long)
	 */
	@Override
	public void setProcessors(long processors)
	{
		this.processors = processors;
	}

	/* (non-Javadoc)
	 * @see org.iplantc.service.jobs.model.scripts.SubScript#setBatchInstructions(java.lang.String)
	 */
	@Override
	public void setBatchInstructions(String batchInstructions)
	{
		this.batchInstructions = batchInstructions;
	}

	/* (non-Javadoc)
	 * @see org.iplantc.service.jobs.model.scripts.SubScript#getBatchInstructions()
	 */
	@Override
	public String getBatchInstructions()
	{
		return batchInstructions;
	}

	/**
	 * @return the executionSystem
	 */
	public ExecutionSystem getExecutionSystem() {
		return executionSystem;
	}

	/**
	 * @param executionSystem the executionSystem to set
	 */
	public void setExecutionSystem(ExecutionSystem executionSystem) {
		this.executionSystem = executionSystem;
	}

	/**
	 * @return the software
	 */
	public Software getSoftware() {
		return software;
	}

	/**
	 * @param software the software to set
	 */
	public void setSoftware(Software software) {
		this.software = software;
	}

	/**
	 * @return the job being submitted
	 */
	public Job getJob() {
		return job;
	}
	/**
	 * Resolves any user-defined job variables from the input script against the given string.
	 * This is called as a filter to the {@link BatchQueue#getCustomDirectives()} before 
	 * adding to the batch script.
	 * 
	 * @param wrapperTemplate the template contents from which any job attribute macros will be resolved
	 * @return the wrapper template with all job attribute macros resolved.
	 */
	public String resolveMacros(String wrapperTemplate) throws JobMacroResolutionException {
		WrapperTemplateMacroResolver resolver = new WrapperTemplateMacroResolver(getJob(), getExecutionSystem());
		return resolver.resolveJobAttributeMacros(wrapperTemplate);
//		for (WrapperTemplateAttributeVariableType macro: WrapperTemplateAttributeVariableType.values()) {
//			wrapperTemplate = StringUtils.replace(wrapperTemplate, "${" + macro.name() + "}", macro.resolveForJob(job));
//		}
//
//		return wrapperTemplate;
	}

}