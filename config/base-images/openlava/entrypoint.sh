#!/bin/sh
openlava_conf_file=/opt/openlava-3.3/etc/lsf.cluster.openlava
# get the hostname of the host. this is set by the container runtime at every startup
hostname=$(hostname)

# replace hostname in pbs.conf and mom_priv/config
sed -i "s/LSF_HOST/$hostname/" $openlava_conf_file

exec "$@"

