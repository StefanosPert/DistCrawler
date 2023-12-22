package code.distcrawler.storage;

import java.io.Serializable;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class StateValue implements  Serializable {
	private String checkpoint;
	private Date versionDate;
	private long saveDocs;
	private ConcurrentHashMap<String,Date> visitedURLs;
	private LinkedBlockingQueue<QueueElement> frontier;
	public StateValue(String name, Date saveDate, ConcurrentHashMap<String,Date> urls, long numDocs) {
		this.checkpoint=name;
		this.versionDate=saveDate;
		this.saveDocs=numDocs;
		this.visitedURLs=urls;
		this.frontier=new LinkedBlockingQueue<QueueElement>();
	}
	public final  LinkedBlockingQueue<QueueElement> getFrontier() {
		return this.frontier;
	}
	public final String name() {
		return this.checkpoint;
	}
	
	public final long numberOfDocs() {
		return this.saveDocs;
	}
	public final ConcurrentHashMap<String,Date> getURLs() {
		return this.visitedURLs;
	}
	
	public final Date getDate() {
		return this.versionDate;
	}
	
	public void putQueueElement(QueueElement elem) {
		try {
			this.frontier.put(elem);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public String toString() {
		return "[StateValue: name"+this.checkpoint+" date="+this.versionDate.getTime()
		      +" Visited URLs="+this.visitedURLs.size()+" Frontier Size="+this.frontier.size()+"]";
	}
}