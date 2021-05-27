#!/usr/bin/env bash
eval $(ssh-agent)
/go/bin/sftp-relay $@

