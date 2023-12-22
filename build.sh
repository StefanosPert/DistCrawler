#!/bin/bash
mkdir -p ~/.aws
cp credentials ~/.aws/credentials
cd crawler
mvn clean install


