#############################################################
#
# OpenLava Scheduler
# Tag: agaveplatform/openlava:3.3
#
# Build image for OpenLava with openssh server
#
# The image can be built by issuing this command from the
# directory containing this file:
#
#   docker build -t agaveplatform/openlava:3.3 .
#
# https://github.com/agaveplatform/base-images
# https://agaveplatform.org
#
#############################################################

FROM centos:7
LABEL maintainer="deardooley@gmail.com"
LABEL description="OpenLava"
LABEL version=3.3

COPY openlava-3.3-3.x86_64.rpm /openlava-3.3-3.x86_64.rpm

# install openlava
RUN yum install -y openlava-3.3-3.x86_64.rpm openssh-server openssh-clients which jq && \
	yum -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm && \
	yum install -y jq python-pip && \
	pip install supervisor

# create test users
RUN mkdir -p /var/run/sshd && \
    echo "root:root" | chpasswd && \
	adduser "testuser" -m && \
    echo "testuser:testuser" | chpasswd && \
    mkdir -p /home/testuser/.ssh && \
    adduser "testshareuser" -m && \
	echo "testshareuser:testshareuser" | chpasswd && \
	mkdir -p /home/testshareuser/.ssh && \
    adduser "testotheruser" -m && \
	echo "testotheruser:testotheruser" | chpasswd && \
	mkdir -p /home/testotheruser/.ssh

# copy public keys
COPY entrypoint.sh /
COPY keys/id_rsa.pub /home/testuser/.ssh/authorized_keys
COPY lsf.submit /home/testuser/lsf.submit
COPY lsf.cluster.openlava /opt/openlava-3.3/etc/lsf.cluster.openlava
COPY supervisord.conf /etc/supervisord.conf

# renaming default queue to debug for testing
RUN sed -i 's#QUEUE_NAME   = normal#QUEUE_NAME   = debug#' /opt/openlava-3.3/etc/lsb.queues && \
	sed -i 's|CPULIMIT     = 180/apple|CPULIMIT     = 1440|' /opt/openlava-3.3/etc/lsb.queues

# distribute public key so test users can login
RUN cp /home/testuser/.ssh/authorized_keys /home/testshareuser/.ssh/authorized_keys && \
	cp /home/testuser/.ssh/authorized_keys /home/testotheruser/.ssh/authorized_keys && \
	cp /home/testuser/lsf.submit /home/testshareuser/lsf.submit && \
	cp /home/testuser/lsf.submit /home/testshareuser/lsf.submit && \
	chown -R testuser:testuser /home/testuser && \
	chown -R testshareuser:testshareuser /home/testshareuser && \
	chown -R testotheruser:testotheruser /home/testotheruser && \
    # generate host keys for sshd \
    /usr/bin/ssh-keygen -A && \
    sed -ri 's/UsePAM yes/#UsePAM yes/g' /etc/ssh/sshd_config && \
    sed -ri 's/#UsePAM no/UsePAM no/g' /etc/ssh/sshd_config && \
    # create temp directories \
    mkdir -p /scratch /work && \
  	chmod -R 777 /scratch /work && \
  	chmod +x /entrypoint.sh && \
    # supervisor log file
  	mkdir -p /var/log/supervisor

EXPOSE 10389 22

ENTRYPOINT ["bash", "/entrypoint.sh"]

CMD ["/usr/bin/supervisord"]