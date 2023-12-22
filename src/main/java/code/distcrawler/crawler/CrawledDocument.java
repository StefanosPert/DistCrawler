package code.distcrawler.crawler;

import java.io.UnsupportedEncodingException;

public class CrawledDocument {
	private byte[] body;
	private String contentType="text/plain";
	private boolean isNew=false;
	public CrawledDocument(byte[] b) {
		this.body=b;
	}
	
	public String body() {
        try {
            return body == null ? "" : new String(body, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return "";
        }
    }
    
    public byte[] bodyRaw() {
        return body;
    }
    
    public void bodyRaw(byte[] b) {
        body = b;
    }
    public void setNew(boolean value) {
    	this.isNew=value;
    }
    public void setType(String type) {
    	this.contentType=type;
    }
    public String getType() {
    	return this.contentType;
    }
    public boolean isNew() {
    	return this.isNew;
    }
}
