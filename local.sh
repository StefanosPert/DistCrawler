#!/bin/bash
mkdir -p ~/.aws
cp credentials ~/.aws/credentials
cd crawler
mvn clean install


echo "export ID=\"1\" && export MAX=\"2\" ">> ~/.bashrc
