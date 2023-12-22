package code.distcrawler.crawler;

import java.io.UnsupportedEncodingException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Set;

import code.distcrawler.crawler.info.URLInfo;

import static code.distcrawler.crawler.MyUtils.*;

// Class containing the meta data and the content of an HTTP response
public class Response {
	private HashMap<String,String> headerMap=new HashMap<String,String>();
	private byte[] body;
	private Date modifiedDate=null;
	private int responseCode=500;
	private URLInfo url=null;
	private boolean newDoc=true;
	
	public void setURL(URLInfo inURL) {
		this.url=inURL;
	}
	
	public URLInfo getURL() {
		return this.url;
	}
    public void setheader(String inputName, String message) {
    	if(this.headerMap.containsKey(inputName) && !inputName.contentEquals("cookie")) {
    		this.headerMap.put(inputName, this.headerMap.get(inputName)+", "+message);
    	}else {
    		this.headerMap.put(inputName, message);
    	}
    }
    public void setCode(int code) {
    	this.responseCode=code;
    }
    
    public int getResponseCode() {
    	return this.responseCode;
    }
    
    public String header(String name) {
    	return this.headerMap.get(name);
    }
    public void setDate(Date date) {
    	this.modifiedDate=date;
    }
    
    public String getContentType() {
       String type=this.header("content-type");
       if(type!=null) {
    	return type.split(";")[0].trim();
       }else {
    	return "text/plain"; 
       }
    }
    public Date lastModified() {
    	if(this.modifiedDate==null) {
    		String lastmodified=this.header("last-modified");
    		if(lastmodified!=null) {
    			this.modifiedDate=dateparser(lastmodified);
    		}else {
    			this.modifiedDate=new Date();
    		}
    		
    	}
    	return this.modifiedDate;
    }
    public Set<String> headers() {
        return this.headerMap.keySet();
      }
    
    public int contentLength() {
        String length=this.header("content-length");
        if(length!=null) {
      	  return Integer.parseInt(length);
        }
        return 0;
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
    
    public void setNewDoc(boolean value) {
    	this.newDoc=value;
    }
    public boolean isNewDoc() {
    	return this.newDoc;
    }
    
}
