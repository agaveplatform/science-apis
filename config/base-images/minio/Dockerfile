#############################################################
#
# Minimal minio image
# Tag: agaveplatform/minio:latest
#
# Minimal SSHD image for integration testing on the Agave
# Platform. Standard test accounts are present with ssh and
# ssh key login.
#
# The image can be built by issuing this command from the
# directory containing this file:
#
#   docker build -t agaveplatform/minio:latest .
#
# https://github.com/agaveplatform/docker-minio
# https://agaveplatform.org
#
#############################################################

FROM minio/minio:RELEASE.2019-10-12T01-39-57Z

RUN minio server /data

