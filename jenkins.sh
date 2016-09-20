#!/usr/bin/env bash

# Create Toil venv
rm -rf .env
virtualenv --no-download .env
. .env/bin/activate

# Prepare directory for temp files
TMPDIR=/mnt/ephemeral/tmp
rm -rf $TMPDIR
mkdir $TMPDIR
export TMPDIR

make prepare
make develop
make test
make pypi
make clean

rm -rf .env $TMPDIR
