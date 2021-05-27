#############################################################
#
# Minimal sshd image
# Tag: agaveplatform/sshd:latest
#
# Minimal SSHD image for integration testing on the Agave
# Platform. Standard test accounts are present with ssh and
# ssh key login.
#
# The image can be built by issuing this command from the
# directory containing this file:
#
#   docker build -t agaveplatform/sshd:latest .
#
# https://github.com/agaveplatform/docker-maven
# https://agaveplatform.org
#
#############################################################

FROM sickp/alpine-sshd:7.9

MAINTAINER Agave Platform <help@agaveplatform.org>

COPY keys/ /home/testuser/.ssh/
COPY keys/ /home/testshareuser/.ssh/
COPY keys/ /home/testotheruser/.ssh/

RUN \
  apk --no-cache add bash && \

  # enable kbi login
  sed -i s/#PermitRootLogin.*/PermitRootLogin\ no/ /etc/ssh/sshd_config && \
  sed -i s/#ChallengeResponseAuthentication.*/ChallengeResponseAuthentication\ yes/ /etc/ssh/sshd_config && \

  # unset root password
  passwd -d root && \

  # add test group for shared acl
  addgroup testers && \
  # add test users
  adduser -D -s /bin/bash testuser && \
  echo "testuser:testuser" | chpasswd && \
  cp /home/testuser/.ssh/id_rsa.pub /home/testuser/.ssh/authorized_keys
  chown -R testuser:testuser /home/testuser && \
  adduser -D -s /bin/bash testshareuser && \
  echo "testshareuser:testshareuser" | chpasswd && \
  cp /home/testshareuser/.ssh/id_rsa.pub /home/testshareuser/.ssh/authorized_keys
  chown -R testshareuser:testshareuser /home/testshareuser && \
  adduser -D -s /bin/bash testotheruser && \
  echo "testotheruser:testotheruser" | chpasswd && \
  cp /home/testotheruser/.ssh/id_rsa.pub /home/testotheruser/.ssh/authorized_keys
  chown -R testotheruser:testotheruser /home/testotheruser && \

  # create /scratch and /work for testing
  mkdir -p /scratch /work && \
  chmod -R 777 /scratch /work && \

  # clean up apk cache
  rm -rf /var/cache/apk/*
