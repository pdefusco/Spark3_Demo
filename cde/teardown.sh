#!/bin/sh

cde_user=$1
max_participants=$2

echo "CDE BNK HOL DB TEARDOWN INITIATED...."
echo "..."
echo ".."
echo "."
echo "Provided CDE User: "

echo "Upload teardown script to resource ice-demo-setup-"$cde_user
cde resource upload --name ice-demo-setup-$cde_user --local-path teardown.py
echo "Create teardown job teardown-"$cde_user
cde job create --name teardown-$cde_user --type spark --mount-1-resource ice-demo-setup-$cde_user --application-file teardown.py
echo "Run teardown job teardown-"$cde_user
cde job run --name teardown-$cde_user
