package code.distcrawler.storage;

import java.io.Serializable;
import java.util.Date;

public class PageValue implements  Serializable {
	private String hostname;
	private String path;
	private Date versionDate;
	private byte[] content;
	private String content_type;
	private String parentLink;
	public PageValue(String host, String pathTofile,String parent, Date accessDate, byte[] body,String type) {
		this.hostname=host;
		this.parentLink=parent;
		this.path=pathTofile;
		this.versionDate=accessDate;
		this.content=body;
		this.content_type=type;
	}
	public final String getParent() {
		return this.parentLink;
	}
	public final String getHost() {
		return this.hostname;
	}
	
	public final String getPath() {
		return this.path;
	}
	
	public final Date getDate() {
		return this.versionDate;
	}
	
	public final byte[] getContent() {
		return this.content;
	}
	
	public final String getType() {
		return this.content_type;
	}
	
	public String toString() {
		return "[PageValue: hostname"+this.hostname+" path="+this.path+" content_type="+this.content_type+"]";
	}
}
