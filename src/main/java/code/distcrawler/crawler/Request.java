package code.distcrawler.crawler;

import java.util.ArrayList;

import code.distcrawler.crawler.info.URLInfo;

// Class containing the meta data for an HTTP request
public class Request{
	private ArrayList<String> headers=new ArrayList<String>();
	private String type="GET";
	private String protocol="HTTP/1.1";
	private URLInfo url=null;
	public void addHeader(String header, String value) {
		this.headers.add(header+": "+value);
	}
	public String getheaders() {
		String result="";
		for(String elem: this.headers) {
			result=result+elem+"\r\n";
		}
		return result;
	}
	public void setURL(URLInfo url_req) {
		this.url=url_req;
	}
	public void setProtocol(String prot) {
		this.protocol=prot;
	}
	public void setType(String in_type) {
		this.type=in_type;
	}
	public String initLine() {
		return type+" "+this.url.getFilePath()+" "+protocol+"\r\n";
	}
	public String requestString() {
		return this.initLine()+this.getheaders()+"\r\n";
	}
}
