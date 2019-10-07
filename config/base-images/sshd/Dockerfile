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

FROM sickp/alpine-sshd:latest

MAINTAINER Agave Platform <help@agaveplatform.org>

ADD keys/id_rsa.pub /home/testuser/.ssh/authorized_keys
ADD keys/id_rsa.pub /home/testshareuser/.ssh/authorized_keys
ADD keys/id_rsa.pub /home/testotheruser/.ssh/authorized_keys

RUN \
  apk add -U --no-cache bash && \
  passwd -d root && \
  sed -i s/#PermitRootLogin.*/PermitRootLogin\ no/ /etc/ssh/sshd_config && \
  sed -i s/#ChallengeResponseAuthentication.*/ChallengeResponseAuthentication\ yes/ /etc/ssh/sshd_config && \
  adduser -D -s /bin/ash testuser && \
  echo "testuser:testuser" | chpasswd && \
  chown -R testuser:testuser /home/testuser && \
  adduser -D -s /bin/ash testshareuser && \
  echo "testshareuser:testshareuser" | chpasswd && \
  chown -R testshareuser:testshareuser /home/testshareuser && \
  adduser -D -s /bin/ash testotheruser && \
  echo "testotheruser:testotheruser" | chpasswd && \
  chown -R testotheruser:testotheruser /home/testotheruser
