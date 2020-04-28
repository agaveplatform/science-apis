#!/bin/bash

PASS=${MONGODB_ADMIN_PASS:-$(pwgen -s 12 1)}
_word=$( [ ${MONGODB_ADMIN_PASS} ] && echo "preset" || echo "random" )

# Set up the default standard user and password to match the dev agave user
# accounts specified in the docker-compose file and maven settings.
USERNAME=${MONGODB_USERNAME:-$(echo 'agaveuser')}
USERPASS=${MONGODB_PASSWORD:-$(echo 'password')}
DATABASE=${d:-$(echo 'api')}

RET=1
while [[ RET -ne 0 ]]; do
    echo "=> Waiting for confirmation of MongoDB service startup"
    sleep 5
    mongo admin --eval "help" >/dev/null 2>&1
    RET=$?
done

echo "=> Creating an admin user with a ${_word} password in MongoDB"
mongo admin --eval "db.createUser({user: 'admin', pwd: '$PASS', roles:[{role:'root',db:'admin'}]});"

_uword=$( [ ${MONGODB_PASSWORD} ] && echo "preset" || echo "default" )
echo "=> Creating a(n) ${USERNAME} user with a ${_uword} password in MongoDB"

sed -i -e "s/ADMIN_PASSWORD/$PASS/" /add_user.js
sed -i -e "s/USERNAME/$USERNAME/" /add_user.js
sed -i -e "s/USERPASS/$USERPASS/" /add_user.js
sed -i -e "s/DATABASE/$DATABASE/" /add_user.js

mongo admin -u admin -p ${PASS} /add_user.js

echo "=> Done!"
touch /data/db/.mongodb_password_set

echo "========================================================================"
echo "You can now connect to this MongoDB server using:"
echo ""
echo "    mongo admin -u admin -p $PASS --host <host> --port <port>"
echo ""
echo "    or"
echo ""
echo "    mongo ${DATABASE} -u ${USERNAME} -p ${USERPASS} --host <host> --port <port>"
echo ""
echo "Please remember to change the above password as soon as possible!"
echo "========================================================================"
