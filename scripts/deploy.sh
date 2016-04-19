#!/bin/bash

while [ 1 = 1 ]; do inotifywait .;scp -r . cstars01e01-par.storage.criteo.preprod:cassback2;scp -r . cstars01e02-par.storage.criteo.preprod:cassback2;done
