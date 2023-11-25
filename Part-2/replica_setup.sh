#!/bin/bash

# This script is used to setup the replica set for the mongoDB cluster

# Check if the dirs exist
if [ -d "./rs1" ]; then
  rm -rf ./rs1
fi

if [ -d "./rs2" ]; then
  rm -rf ./rs2
fi

if [ -d "./rs3" ]; then
  rm -rf ./rs3
fi

# Create the dirs
mkdir ./rs1
mkdir ./rs2
mkdir ./rs3

# Start the replica set
mongod --replSet "SmartBuilding" --logpath "./rs1/1.log" --dbpath "./rs1" --port 27017 &
mongod --replSet "SmartBuilding" --logpath "./rs2/2.log" --dbpath "./rs2" --port 27018 &
mongod --replSet "SmartBuilding" --logpath "./rs3/3.log" --dbpath "./rs3" --port 27019 &



