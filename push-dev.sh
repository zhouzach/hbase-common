#!/bin/bash

mvn clean package

rsync -P -v -e "sshpass -p 're%^hoodoptest#2020!@#dis' ssh -l root" ./target/hbase-common-1.0.jar cdh3:/data/warehouse/storage

