
## Distributed Website Crawler
This is a distributed crawler that can be deployed in AWS, where each crawler head corresponds to one ec2 instance. Part of the code was developed during the CIS 555 course of "Internet and Web Systems" of UPenn

### Deployment instructions

The crawler require 3 Dynamo DB tables. StartingTime with crawlerId key, visitedURLs with URL key, UrlTable with HashedUrl key

Create a file called credentials with the aws session credentials
Configure uploading.sh by putting the path of the source code and the AWS private key
<br> Run the following
```
./uploading.sh hostIPA hostIPB 
```
Where hostIPA, hostIPB are the IPs of the EC2 instances. This will open one new terminal for each instance (gnome-terminal is required). This examples setups 2 individual crawler head, but all the instructions are generalizable to an arbitrary number of crawler heads.

After the terminal of each EC2 instance opens, run the following combination of commands in each terminal
```
sudo chmod 755 settingup.sh && sudo ./settingup.sh && sudo chmod 755 build.sh && ./build.sh &&  . ~/.bashrc  && PS1=$ && PROMPT_COMMAND= && echo -en "\033]0;$ID:$MAX\a" && export PS1='\u@\h:\w\$ '
```

Finally to start the crawler heads and initiate the crawling with starting points the webpages provided in the frontier, run the following instruction in each head's terminal (it is recommended to start from the head with ID=0)
```
cd ~/crawler && mvn exec:java -Dexec.mainClass="code.distcrawler.crawler.XPathCrawler" -Dexec.args="/home/ec2-user/frontier  /home/ec2-user  1 2000 20000 cis $ID:$MAX <S3-bucket-name>"
```
where you should replace \<S3-bucket-name\> with the name of the S3 bucket you want to save the results.

To continue the crawl run:
```
cd ~/crawler && mvn exec:java -Dexec.mainClass="code.distcrawler.crawler.XPathCrawler" -Dexec.args="continue  /home/ec2-user  1 2000 20000 cis $ID:$MAX <S3-bucket-name>"
```