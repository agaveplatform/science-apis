package org.iplantc.service.systems.model.enumerations;

import org.iplantc.service.systems.model.ExecutionSystem;

/**
 * Provides enumerated representations of the Linux shells used on {@link ExecutionSystem}. These help us source
 * the proper files upon job execution in user space.
 */
public enum RemoteShell
{
	BASH, TCSH, CSH, ZSH;

	/**
	 * Returns the default run command (.*rc) file for each known remote shell. When a startup script is not
	 * provided in a {@link ExecutionSystem} or Software definition, these will be run prior to remote command
	 * execution on the host.
	 *
	 * @return path to the rc file for this {@link RemoteShell}
	 */
	public String getDefaultRCFile() {
		if (this == CSH) {
			return "~/.cshrc";
		} else if (this == TCSH) {
			return "~/.tcsh";
		} else if (this == ZSH) {
			return "~/.zsh";
		} else {
			return "~/.bashrc";
		}
	}
}
