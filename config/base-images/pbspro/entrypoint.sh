#!/bin/sh
pbs_conf_file=/etc/pbs.conf
mom_conf_file=/var/spool/pbs/mom_priv/config
# get the hostname of the host. this is set by the container runtime at every startup
hostname=$(hostname)

# replace hostname in pbs.conf and mom_priv/config
sed -i "s/PBS_SERVER=.*/PBS_SERVER=$hostname/" $pbs_conf_file
sed -i "s/\$clienthost .*/\$clienthost $hostname/" $mom_conf_file

# start sshd
/usr/sbin/sshd -D &

# start PBS Pro
/etc/init.d/pbs start

# init default node and queue
/opt/pbs/bin/qmgr -c "create node $hostname"
/opt/pbs/bin/qmgr -c "create queue debug queue_type=execution"
/opt/pbs/bin/qmgr -c "set queue debug enabled=true"
/opt/pbs/bin/qmgr -c "set queue debug started=true"
/opt/pbs/bin/qmgr -c 'set queue debug resources_default.walltime = 48:00:00'
/opt/pbs/bin/qmgr -c 'set queue debug resources_default.nodes = 1'

# enable job history for 2 weeks
/opt/pbs/bin/qmgr -c "set server job_history_enable=true"
/opt/pbs/bin/qmgr -c "set server job_history_duration=336:00:00"

# enable scheduling and assign the debug queue as the default for the submit node
/opt/pbs/bin/qmgr -c "set server scheduling=True"
/opt/pbs/bin/qmgr -c "set node $hostname queue=debug"

exec "$@"

