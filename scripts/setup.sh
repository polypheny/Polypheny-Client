#!/bin/bash

####################
# This script is intended to be used on a node which runs Ubuntu 16.04
# It installs docker, java, deploys the jar and installs netdata.
####################

echo "update & upgrade"
sudo apt clean
sudo apt -qq update
sudo apt -y -qq upgrade
sudo apt clean
echo "update & upgrade done"

sudo apt -y -qq install apt-transport-https curl vim apt-utils software-properties-common git ca-certificates

#Docker
 curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
 sudo add-apt-repository \
    "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
    $(lsb_release -cs) \
    stable"
sudo apt update
sudo apt -y -qq install docker-ce
sudo groupadd docker
sudo usermod -aG docker ubuntu

#Java
sudo add-apt-repository -y ppa:webupd8team/java && sudo apt-get update; \
    sudo echo debconf shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && sudo  echo debconf shared/accepted-oracle-license-v1-1 seen true | debconf-set-selections; \
    sudo apt -y -qq install oracle-java8-installer; \
    cd /usr/lib && sudo ln -s jvm/java-8-oracle java
export JAVA_HOME=/usr/lib/java/jre

#Build jar
cd /home/ubuntu/polypheny-db-client/
./gradlew generateProto
./gradlew shadowJar
./scripts/setup-docker.sh
./scripts/netdata-setup.sh