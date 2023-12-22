package code.distcrawler.storage;

import java.io.Serializable;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

import code.distcrawler.crawler.CrawledDocument;

public class ChannelValue implements Serializable{
	private String channelName;
	private HashSet<String> channelMap=new HashSet<String>();
	private String creator;
	private String xpath;
	private Date creationDate;
	
	public ChannelValue(String name,String path, String user) {
		this.channelName=name;
		this.creator=user;
		this.creationDate=new Date();
		this.xpath=path;
	}
	public final String getXPath() {
		return this.xpath;
	}
	
	public final String getName() {
		return this.channelName;
	}
	
	public final String getCreator() {
		return this.creator;
	}
	
	public final Date getCreationDate() {
		return this.creationDate;
	}
	
	public HashSet<String> getSet(){
		return this.channelMap;
	}
	public void addURL(String key) {
		synchronized(this.channelMap) {
		this.channelMap.add(key);
		}
	}
	public void resetURL() {
		this.channelMap=new HashSet<String>();
	}
	
	public String toString() {
		return "[ChannelValue: channelName="+this.channelName+" User="+this.creator+" XPath="+this.xpath+"]";
	}
}
