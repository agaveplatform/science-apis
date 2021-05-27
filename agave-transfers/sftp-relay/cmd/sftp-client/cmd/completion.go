/*
Copyright © 2021 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"github.com/spf13/cobra"
	"os"
)

// completionCmd represents the completion command
var completionCmd = &cobra.Command{
	Use:   "completion [bash|zsh|fish|powershell]",
	Short: "Generate completion script",
	DisableFlagsInUseLine: false,
	ValidArgs:             []string{"bash", "zsh", "fish", "powershell"},
	Args:                  cobra.ExactValidArgs(1),
	Long: `To load completions:

Bash:

  $ source <(sftp-client completion bash)

  # To load completions for each session, execute once:
  # Linux:
  $ sftp-client completion bash > /etc/bash_completion.d/sftp-client
  # macOS:
  $ sftp-client completion bash > /usr/local/etc/bash_completion.d/sftp-client

Zsh:

  # If shell completion is not already enabled in your environment,
  # you will need to enable it.  You can execute the following once:

  $ echo "autoload -U compinit; compinit" >> ~/.zshrc

  # To load completions for each session, execute once:
  $ sftp-client completion zsh > "${fpath[1]}/_sftp-client"

  # You will need to start a new shell for this setup to take effect.

fish:

  $ sftp-client completion fish | source

  # To load completions for each session, execute once:
  $ sftp-client completion fish > ~/.config/fish/completions/sftp-client.fish

PowerShell:

  PS> sftp-client completion powershell | Out-String | Invoke-Expression

  # To load completions for every new session, run:
  PS> sftp-client completion powershell > sftp-client.ps1
  # and source this file from your PowerShell profile.
`,
	Run: func(cmd *cobra.Command, args []string) {
		switch args[0] {
		case "bash":
			cmd.Root().GenBashCompletion(os.Stdout)
		case "zsh":
			cmd.Root().GenZshCompletion(os.Stdout)
		case "fish":
			cmd.Root().GenFishCompletion(os.Stdout, true)
		case "powershell":
			cmd.Root().GenPowerShellCompletion(os.Stdout)
		}
	},
}

func init() {
	rootCmd.AddCommand(completionCmd)
}
