package code.distcrawler.storage;

import java.io.Serializable;

public class PageKey implements Serializable{
	private String hostname;
	private String path;
	private String protocol;
	private int port;
	public PageKey(String host,String pathTofile,int urlPort,String URLprotocol) {
		this.hostname=host;
		this.path=pathTofile;
		this.port=urlPort;
		this.protocol=URLprotocol;
	}
	public final String getHost() {
		return this.hostname;
	}
	public final String getProtocol() {
		return this.protocol;
	}
	public final String getPath() {
		return this.path;
	}
	public final int getPort() {
		return this.port;
	}
	
	public String toString() {
		return "[PageKey: hostname="+this.hostname+" path="+this.path+" port="+String.valueOf(this.port)+" protocol="+this.protocol+"]";
	}
}
