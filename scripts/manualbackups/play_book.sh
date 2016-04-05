#!/bin/bash

PLAYBOOK=$1

if [ "$PLAYBOOK" = "" ]; then
	echo "Usage: $0 <playbook> [ansible options]"
	exit 65
fi

shift
ansible-playbook --inventory-file=inventory.txt playbooks/$PLAYBOOK.yml --extra-vars $*

exit $?
