#!/bin/bash
key="gitUpload.pem"
setup="settingup.sh"
build="build.sh"
frontier="frontier"
cred="credentials"
code="../crawler"
MAX="$#"
HEADS="2"
i=0
echo "Working"
for var in "$@"
do
	cat $build > local.sh
	echo "echo \"export ID=\\\"$i\\\" && export MAX=\\\"$MAX\\\" \">> ~/.bashrc" >> local.sh
	#echo "Going for the copy"
	scp -o StrictHostKeyChecking=no  -i $key $setup ec2-user@$var:.
	scp -i $key local.sh  ec2-user@$var:$build
	scp -i $key $frontier ec2-user@$var:.
	scp -i $key $cred ec2-user@$var:
	scp -r -i $key $code ec2-user@$var:.

	gnome-terminal --command  "bash -c \"ssh -t -i $key ec2-user@$var ; exec bash \""
	 ((i=i+1))
done

