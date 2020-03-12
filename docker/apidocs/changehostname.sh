#!/bin/bash

sed -i 's#https://sandbox.agaveplatform.org/docs/v2/resources#http://'$(hostname)'/docs/resources#' /var/www/html/docs/resources/index
for i in `ls /var/www/html/docs/resources/`
do
  sed -i 's#"https://sandbox.agaveplatform.org"#"https://'$(hostname)'"#g' /var/www/html/docs/resources/$i
done
