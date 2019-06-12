#!/bin/bash

####################
# stops and removes all containers and the polyphenynw. Does NOT remove the installed images.
####################

docker stop postgresql
docker rm postgresql
docker network rm polyphenynw