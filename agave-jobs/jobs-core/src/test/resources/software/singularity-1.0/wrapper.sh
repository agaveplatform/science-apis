#!/bin/bash

handle_trap() {
	rc=$?
	set +x
	if [ "$rc" != 0 ]
	then
	  true # this command does nothing
	  $(${AGAVE_JOB_CALLBACK_FAILURE})
	fi
	echo "EXIT($rc)" > run_dir/return_code.txt
	tar czf output.tgz run_dir
}
trap handle_trap ERR EXIT
set -ex

tar xzvf ${input tarball}
echo "export AGAVE_JOB_NODE_COUNT=${AGAVE_JOB_NODE_COUNT}" > .env
echo "export AGAVE_JOB_PROCESSORS_PER_NODE=${AGAVE_JOB_PROCESSORS_PER_NODE}" >> .env
echo "export nx=${nx}" >> .env
echo "export ny=${ny}" >> .env
echo "export nz=${nz}" >> .env
(cd ./run_dir && source ./runapp.sh)