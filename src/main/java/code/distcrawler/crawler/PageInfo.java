package code.distcrawler.crawler;

import java.io.Serializable;
import java.util.Date;

public class PageInfo implements Serializable {
	private static final long serialVersionUID = 12L;
	public String URL;
	public String contentType;
	public Date date;
	public long start;
	public long end;
	public String parentURL;
	public PageInfo() {
		
	}
	public PageInfo(String newURL,Date accessDate,long startPos,
			long endPos,String parent,String type) {
		super();
		this.URL=newURL;
		this.date=accessDate;
		this.start=startPos;
		this.end=endPos;
		this.contentType=type;
		this.parentURL=parent;
	}
}
