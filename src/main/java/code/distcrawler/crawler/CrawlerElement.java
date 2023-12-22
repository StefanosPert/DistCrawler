package code.distcrawler.crawler;

import java.util.Date;

import code.distcrawler.crawler.info.URLInfo;
import java.io.Serializable;

// Class of crawler elements that have to wait until a certain time WaitUntil. Elements are order
// according to the WaitUntil time and in the case of ties the element that is initialized first is
// the smallest
public class CrawlerElement implements Serializable {
	private static final long serialVersionUID = 1L;
	public long delay;
	public long timeOfEntry;
	static public long running_id=0;
	public long instance_id;
	public String normUrl;
	public String parent;
	public CrawlerElement() {
		
	}
	public CrawlerElement(long waitdelay, long time, URLInfo urlToCrawl) {
		this.delay=waitdelay;
		this.timeOfEntry=time;
		this.instance_id=running_id;
		running_id++;
		this.normUrl=urlToCrawl.getNormalized();
		this.parent=urlToCrawl.getParent();
	}
	
	public long getDelay() {
		return this.delay;
	}
	public void setDelay(long waitdelay) {
		this.delay=waitdelay;
	}
	
	public long getTimeOfEntry() {
		return this.timeOfEntry;
	}
	public void setTimeOfEntry(long time) {
		this.timeOfEntry=time;
	}
	/*
	public URLInfo getURL() {
		return this.url;
	}
	*/
	public void setURL(URLInfo urlToCrawl) {
		this.normUrl=urlToCrawl.getNormalized();
		this.parent=urlToCrawl.getParent();
	}
	public long WaitUntil() {
		return this.timeOfEntry+this.delay;
	}
	
	public int compareTo(CrawlerElement other) {
		long waitDif=this.WaitUntil()-other.WaitUntil();
		long entryDif=this.instance_id-other.instance_id;
		if(waitDif<0) {
			return -1;
		}else if(waitDif>0) {
			return 1;
		}else {
			if(entryDif<0) {
				return -1;
			}else if(entryDif>0) {
				return 1;
			}else {
				return 0;
			}
		}
	}
	
}
