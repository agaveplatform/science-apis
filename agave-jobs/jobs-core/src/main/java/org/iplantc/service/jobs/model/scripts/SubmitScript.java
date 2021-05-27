/**
 * 
 */
package org.iplantc.service.jobs.model.scripts;

import org.iplantc.service.apps.model.enumerations.ParallelismType;
import org.iplantc.service.jobs.exceptions.JobException;
import org.iplantc.service.jobs.exceptions.JobMacroResolutionException;

/**
 * @author dooley
 * 
 */
public interface SubmitScript {

	String getScriptText() throws JobException, JobMacroResolutionException;
	
	/**
	 * @return the name
	 */
    String getName();

	/**
	 * @param name
	 *            the name to set
	 */
    void setName(String name);

	/**
	 * @return the inCurrentWorkingDirectory
	 */
    boolean isInCurrentWorkingDirectory();

	/**
	 * @param inCurrentWorkingDirectory
	 *            the inCurrentWorkingDirectory to set
	 */
    void setInCurrentWorkingDirectory(
            boolean inCurrentWorkingDirectory);

	/**
	 * @return the verbose
	 */
    boolean isVerbose();

	/**
	 * @param verbose
	 *            the verbose to set
	 */
    void setVerbose(boolean verbose);

	/**
	 * @return the standardOutputFile
	 */
    String getStandardOutputFile();

	/**
	 * @param standardOutputFile
	 *            the standardOutputFile to set
	 */
    void setStandardOutputFile(String standardOutputFile);

	/**
	 * @return the standardErrorFile
	 */
    String getStandardErrorFile();

	/**
	 * @param standardErrorFile
	 *            the standardErrorFile to set
	 */
    void setStandardErrorFile(String standardErrorFile);

	/**
	 * @return the time
	 */
    String getTime();

	/**
	 * @param time
	 *            the time to set
	 */
    void setTime(String time);

	/**
	 * @return the parallel
	 */
    ParallelismType getParallelismType();

	/**
	 * @param parallelismType
	 *            the parallelismType to set
	 */
    void setParallelismType(ParallelismType parallelismType);

	/**
	 * @return the processors
	 */
    long getProcessors();

	/**
	 * @param processors
	 *            the processors to set
	 */
    void setProcessors(long processors);

	/**
	 * @param batchInstructions
	 *            the batchInstructions to set
	 */
    void setBatchInstructions(String batchInstructions);

	/**
	 * @return the batchInstructions
	 */
    String getBatchInstructions();
	
}
