######################################################
#
# Agave DevOps Slurm Server release tag slurm-20-02-3-1
# Tag: agaveplatform/slurm:20.02.3
#
# This container provides a custom Slurm controller and
# and worker created on top of ubuntu:18.04 to provide a
# functional job submission container for integration testing.
# Standard test user account and keys are deployed. SSH server
# is made available for remoting.
#
# Usage:
# docker run -it --rm \
#            -p 10033:22                  \ # SSH, SFTP
#            --rm agaveplatform/slurm:20.02.3
#
#  https://github.com/agaveplatform/base-images
#  https://agaveplatform.org
#
######################################################

FROM ubuntu:18.04

LABEL maintainer="Rion Dooley <deardooley@gmail.com>"

# Add slurm user
RUN adduser slurm && \
    echo "slurm:slurm" | chpasswd
USER slurm
RUN mkdir /home/slurm/.ssh
ADD ssh/id_rsa.pub /home/slurm/.ssh/authorized_keys
USER root

# Install slurm
RUN apt-get update && \
	apt-get install -y git bzip2 vim-tiny munge openssh-server gcc make tar libmunge-dev g++ && \
	chsh --shell /bin/bash munge

RUN git clone --depth 1 -b slurm-20-02-3-1 https://github.com/SchedMD/slurm.git /usr/local/slurm  && \
	cd /usr/local/slurm && \
	./configure --prefix=/usr --sysconfdir=/etc/sysconfig/slurm --with-mysql_config=/usr/local/bin && \
	make && \
	make install

RUN apt-get -y remove gcc g++ && \
	apt-get -y autoremove

RUN	chown -R root:root /var/log/munge && \
	chown -R root:root /var/lib/munge && \
	mkdir -p /var/run/munge && \
	chown -R root:root /var/run/munge && \
	chown -R root:root /etc/munge && \

	mkdir /var/log/slurm && \
	touch /var/log/slurm/job_completions && \
	touch /var/log/slurm/accounting && \
	chown -R slurm:slurm /var/log/slurm && \

	touch /var/spool/last_config_lite && \
	touch /var/spool/last_config_lite.new && \
	chown slurm:slurm /var/spool/last_config_lite* && \

	chown root:slurm /var/spool && \
	chmod g+w /var/spool && \
	mkdir -p /scratch /work

RUN adduser --shell /bin/bash "testuser"  && \
	echo "testuser:testuser" | chpasswd
USER testuser
RUN mkdir /home/testuser/.ssh
COPY ssh/id_rsa.pub /home/testuser/.ssh/authorized_keys
COPY slurm/slurm.submit /home/testuser/slurm.submit
USER root

RUN adduser --shell /bin/bash "testshareuser"  && \
    echo "testshareuser:testshareuser" | chpasswd
USER testshareuser
RUN mkdir /home/testshareuser/.ssh
COPY ssh/id_rsa.pub /home/testshareuser/.ssh/authorized_keys
USER root

RUN adduser --shell /bin/bash "testotheruser" && \
    echo "testotheruser:testotheruser" | chpasswd
USER testotheruser
RUN mkdir /home/testotheruser/.ssh
COPY ssh/id_rsa.pub /home/testotheruser/.ssh/authorized_keys
USER root

COPY docker-entrypoint.sh /docker-entrypoint.sh

EXPOSE 8218 8217 22

ENTRYPOINT ["/docker-entrypoint.sh"]