######################################################
#
# MongoDB 3.0
# Tag: agaveplatform/mongodb:3.0
#
# Base MongoDB 3 image based on vanilla mongo 3 install. Adds support
# for MONGODB_USERNAME, MONGODB_PASSWORD, MONGODB_DATABASE,
# and MONGODB_ADMIN_PASSWORD variables to customize the auth.
#
# If not value are specified, the default Agave agaveuser/password
# credentials are used against the `api` and `notifications`
# databases.
#
# Usage:
# docker run -it --rm \
#            -p 27017:27017
#            -p 28017:28017
#            --rm agaveplatform/mongodb:3.0
#
#  https://github.com/agaveplatform/base-images
#  https://agaveplatform.org
#
######################################################
FROM ubuntu:14.04

LABEL MAINTAINER="Rion Dooley <deardooley@gmail.com>"

RUN apt-key list | \
     grep "expired: " | \
     sed -ne 's|pub .*/\([^ ]*\) .*|\1|gp' | \
     xargs -n1 sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys && \
    echo "deb http://repo.mongodb.org/apt/ubuntu trusty/mongodb-org/3.0 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-3.0.list && \
    apt-get update && \
    apt-get install gnupg && \
    apt-get install -y --force-yes gnupg pwgen mongodb-org mongodb-org-server mongodb-org-shell mongodb-org-mongos mongodb-org-tools

VOLUME /data/db

ENV AUTH yes
ENV STORAGE_ENGINE wiredTiger
ENV JOURNALING yes

ADD run.sh /run.sh
ADD set_mongodb_password.sh /set_mongodb_password.sh
ADD add_user.js /add_user.js

EXPOSE 27017 28017

CMD ["/run.sh"]
