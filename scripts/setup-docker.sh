#!/bin/bash

####################
# creates and sets up a docker-container which runs postgres
####################

docker network create --driver bridge polyphenynw

# creates postgresql container
docker run --net=polyphenynw -p 5432:5432 -e POSTGRES_PASSWORD=polypheny -e POSTGRES_DB=polypheny -e POSTGRES_USER=polypheny  -h postgresql --name postgresql --net-alias postgresql -d postgres:9.5

echo "====================================================="
echo "A postgresql container has been started on port 5432!"
echo "run 'docker stop postgresql' to stop it and 'docker start postgresql' to start it"