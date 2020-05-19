#############################################################
#
# PBS Professional Open Source
# Tag: agaveplatform/pbspro:19.1.2
#
# Build image for pbspro with openssh server
#
# The image can be built by issuing this command from the
# directory containing this file:
#
#   docker build -t agaveplatform/pbspro:19.1.2 .
#
# https://github.com/agaveplatform/docker-golang
# https://agaveplatform.org
#
#############################################################

# Use the officail pbspro/pbsbase image as builder
# build script will be triggered
FROM pbspro/pbsbase:centos7 AS builder
# get latest PBS Pro source code. notice the tagged checkout
RUN git clone https://github.com/pbspro/pbspro.git -b v19.1.2 --depth 1 /src/pbspro
RUN yum install -y postgresql-contrib
RUN bash /src/pbspro/docker/centos7/build.sh

# base image
FROM centos:7
LABEL maintainer="deardooley@gmail.com"
LABEL description="PBS Professional Open Source"
LABEL version=19.1.2

# copy rpm and entrypoint script from builder
COPY --from=builder /root/rpmbuild/RPMS/x86_64/pbspro-server-*.rpm .

# install pbspro
RUN yum install -y pbspro-server-*.rpm openssh-server openssh-clients which jq

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
COPY pbspro.submit /home/testuser/pbspro.submit

# distribute public key so test users can login
RUN cp /home/testuser/.ssh/authorized_keys /home/testshareuser/.ssh/authorized_keys && \
	cp /home/testuser/.ssh/authorized_keys /home/testotheruser/.ssh/authorized_keys && \
	cp /home/testuser/pbspro.submit /home/testshareuser/pbspro.submit && \
	cp /home/testuser/pbspro.submit /home/testshareuser/pbspro.submit && \
	chown -R testuser:testuser /home/testuser && \
	chown -R testshareuser:testshareuser /home/testshareuser && \
	chown -R testotheruser:testotheruser /home/testotheruser && \
    # generate host keys for sshd
    /usr/bin/ssh-keygen -A && \
    sed -ri 's/UsePAM yes/#UsePAM yes/g' /etc/ssh/sshd_config && \
    sed -ri 's/#UsePAM no/UsePAM no/g' /etc/ssh/sshd_config && \
    # create temp directories \
    mkdir -p /scratch /work && \
  	chmod -R 777 /scratch /work && \
  	chmod +x /entrypoint.sh

# set this so the container operates as an execution host out of the box
ENV PBS_START_MOM=1

ENTRYPOINT ["bash", "/entrypoint.sh"]
