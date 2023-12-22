package code.distcrawler.crawler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import javax.net.ssl.HttpsURLConnection;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import code.distcrawler.crawler.info.RobotsTxtInfo;
import code.distcrawler.crawler.info.URLInfo;
import code.distcrawler.storage.DBWrapper;
import code.distcrawler.storage.PageKey;
import code.distcrawler.storage.PageValue;
import static code.distcrawler.crawler.MyUtils.*;

public class CrawlerHttpClient {
	private String monitorURL;
	private InetAddress monitorhost;
	private DatagramSocket monitorsocket;
	private int maxvalue;
	public CrawlerHttpClient(String url, int maxBytes) {
		this.monitorURL=url;
		try {
		this.monitorhost=InetAddress.getByName(url);
		this.monitorsocket=new DatagramSocket();
		}catch(UnknownHostException | SocketException  e) {
			e.printStackTrace();
			this.monitorhost=null;
			this.monitorsocket=null;
		}
		this.maxvalue=maxBytes;
	}
	
	// Parse an http response with method type that is send in the inputReader
	public Response parseResponse( BufferedReader inputReader,String type) {
		Response result=new Response();
		String regex="^\\s+";
		byte[] body;
		try {
			// Read the first line of the response
			String firstline=inputReader.readLine();
			if(firstline!=null) {
				String[] line_parts=firstline.split(" ",-1);
				if(line_parts.length>=3) {
					result.setCode(Integer.valueOf(line_parts[1]));
					
				}
			}
			
			// Parse the individual headers of the response and store them in the response object
			String headers=inputReader.readLine();
			if(headers!=null) {
				while(!headers.trim().contentEquals("")) {
					String[] parts=headers.split(":",2);
					if(parts.length==2) {
						result.setheader(parts[0].toLowerCase(), parts[1].replaceAll(regex, ""));
					}
					headers=inputReader.readLine();
					if(headers==null) {
						break;
					}
				}
			}
			// if content length is unknown set it to be equal to the max value
			int num_char=result.contentLength()<=0 ? this.maxvalue : result.contentLength();
			
			// Read the content of the response, one byte at a time
			if(!type.contentEquals("HEAD") && num_char>0) {

				body=new byte[num_char];
				for(int i=0; i<num_char; i++) {
					int inputbyte=inputReader.read();
					if(inputbyte<0) {
						break;
					}
					body[i]=(byte) inputbyte; 
				}
				
				result.bodyRaw(body);
			}
		}catch(IOException e) {
			e.printStackTrace();
		}
		return result;
	}
	
	// Send a UDP packet to the monitor server, about sending request to the url specified
	// in the url argument
	public void notifyMonitor(URLInfo url) {
		if(url!=null && this.monitorhost!=null && this.monitorsocket!=null) {
			byte[] data=("G12crawler;"+url.getProtocol().toLowerCase()+"://"+url.getHostName()+":"+url.getPortNo()+url.getFilePath()).getBytes();
			DatagramPacket packet=new DatagramPacket(data,data.length,this.monitorhost,10455);
			try {
			this.monitorsocket.send(packet);
			}catch(IOException e){
				e.printStackTrace();
			}
		}	
	}
	
	/* Send an HTTP request to the URL specified by the url argument
	   The method of the request is specified by the string type
	   This function waits for the response to the request and returns a Response
	   objects containing the content of the response and other information regarding
	   the response
	*/
	public Response send_request(URLInfo url,String type) {
		Response response;
		try {
		this.notifyMonitor(url);
		if(!url.isHTTPS()) {
			//HTTP request and response parsing
			Socket socket=new Socket(InetAddress.getByName(url.getHostName()),url.getPortNo());
			socket.setSoTimeout(3000);
			OutputStream output=socket.getOutputStream();
			InputStream input=socket.getInputStream();
		
			PrintWriter writer=new PrintWriter(output,true);
			BufferedReader reader=new BufferedReader(new InputStreamReader(input,StandardCharsets.ISO_8859_1));
			
			// Create Request object containing the meta data for the request
			Request request=new Request();
			request.setURL(url);
			request.setType(type);
			request.addHeader("Host", url.getHostName());
			request.addHeader("User-Agent", "cis455crawler");
			request.addHeader("Connection", "close");
			writer.print(request.requestString());
			writer.flush();
			
			//Parse the response
			response=this.parseResponse(reader,type);
			socket.close();
		}else {
			//HTTPS request and response parsing using java's HttpsURLConnection
			//Open connection and send request
			URL https_url=new URL("https://"+url.getHostName()+url.getFilePath());
			HttpsURLConnection connection=(HttpsURLConnection) https_url.openConnection();
			connection.setConnectTimeout(3000);
			connection.setRequestMethod(type);
			connection.setInstanceFollowRedirects(true);
			connection.setRequestProperty("User-Agent", "cis455crawler");
			response=new Response();
			
			//Create the response object that send_request will return
			response.setCode(connection.getResponseCode());
			response.setDate(new Date(connection.getLastModified()));
			if(connection.getContentType()!=null) {
				response.setheader("content-type", connection.getContentType());}
			String location=connection.getHeaderField("location");
			
			if(location!=null) {
				response.setheader("location", location);
			}
			response.setheader("content-length", String.valueOf(connection.getContentLength()));
			
			// If the content length is not available in the response, set it to the max value
			int num_char=response.contentLength()<=0 ? this.maxvalue : response.contentLength();
			if(!type.contentEquals("HEAD") && num_char>0) {
				BufferedReader reader=new BufferedReader(new InputStreamReader(connection.getInputStream(),StandardCharsets.ISO_8859_1));
				// Read all the bytes of the response one at a time and store in the the response buffer
				byte[] body=new byte[num_char];
				for(int i=0; i<num_char; i++) {
					int inputbyte=reader.read();
					if(inputbyte<0) {
						break;
					}
					body[i]=(byte) inputbyte; 
				}
				
				response.bodyRaw(body);
			}
			
		}
		}catch(IOException  e) {
			response=new Response();
		}
		return response;
	}
	
	// Checks if the mimeType is one of the content types that the crawler should store
	// in the database
	public boolean checkContentType(String mimeType) {
		if(mimeType==null) {
			return false;
		}
		String[] typeParts=mimeType.split("/");
		if(typeParts.length==2) {
			String firstPart=typeParts[0];
			String secPart=typeParts[1];
			if(((firstPart.contentEquals("application") || firstPart.contentEquals("text")) && secPart.contentEquals("xml")) || 
				 secPart.endsWith("+xml") || (
				 secPart.contentEquals("html") && firstPart.contentEquals("text"))) {
				return true;
			}
		}
		return false;
	}
	public void updateRobotsTxtInfo(RobotsTxtInfo robots, URLInfo urlToaccess,String agent) {
		try {
		// This function checks if the host of the URL specified in the urlToaccess has a /robots.txt file and 
		// it update the RobotsTxtInfo object of the crawler accordingly
		URLInfo url=new URLInfo(urlToaccess.getProtocol().toLowerCase()+"://"+urlToaccess.getHostName()+":"+urlToaccess.getPortNo()+"/robots.txt",urlToaccess.getParent());
		Response response=null;
		int resp_code=300;
		int count=0;
		// check for redirects where the maximum number of redirects is 10
		while((resp_code>=300 && resp_code<400) && count<10) {
			count++;
			response=this.send_request(url, "GET");
			resp_code=response.getResponseCode();
			url=response.getURL();
		}
		ArrayList<String> disallowPaths=null;
		int crawler_delay=-1;
		// if /robots.txt exists parse it and update the crawler's RobotsTxtInfo object accordingly
		if(response.getResponseCode()==200) {
			BufferedReader reader=new BufferedReader(new StringReader(response.body()));
			String line=reader.readLine();
			boolean specifiedAgent=false;
			while(line!=null) {
				if(line.toLowerCase().startsWith("user-agent:")) {
					String referredAgent=line.substring(11);
					referredAgent=referredAgent.split("#")[0].trim();
					if((referredAgent.contentEquals(agent) && !specifiedAgent) || (referredAgent.contentEquals("*") && disallowPaths==null)) {
						
						specifiedAgent=referredAgent.contentEquals(agent);
						disallowPaths=new ArrayList<String>();
						
						line=reader.readLine().trim();
						boolean rulelinePresent=false;
						while(line!=null && !line.contentEquals("") && !(line.toLowerCase().startsWith("user-agent:") && rulelinePresent)  ) {
							if(!line.toLowerCase().startsWith("#")) {
								rulelinePresent=true;
							}
							if(line.toLowerCase().startsWith("disallow:")) {
								line=line.substring(9);
								disallowPaths.add(line.split("#")[0].trim());
							}
							if(line.toLowerCase().startsWith("crawl-delay:")) {
								try {
									line=line.substring(12);
									int delay=Integer.valueOf(line.split("#")[0].trim());
									crawler_delay=delay;
								}catch(NumberFormatException e) {
									e.printStackTrace();
								}
							}
							line=reader.readLine();
							
						}
					   continue;
					}
				}
				line=reader.readLine();
			}
			if(disallowPaths!=null) {
				for(String elem: disallowPaths) {
					robots.addDisallowedLink(urlToaccess.getHostName()+":"+urlToaccess.getPortNo(), elem);
				}
		
			}
			if(crawler_delay>0) {
				robots.addCrawlDelay(urlToaccess.getHostName()+":"+urlToaccess.getPortNo(), crawler_delay);
			}
		}
		}catch(Exception e) {
			//e.printStackTrace();
		}
	}
	
	// Send an http request and check if the server response with a redirection
	public Response checkRedirect(URLInfo url, String method) throws Exception{
		Response response=this.send_request(url,method);
		int resp_code=response.getResponseCode();
		if((resp_code>=300 && resp_code<400) ) {
		  if(response.header("location")!=null) {
			url=new URLInfo(response.header("location").trim(),url.getParent());
		  }else { 
			  response.setCode(400);
		  }
		}
		response.setURL(url);
		return response;
	}
	
	/* This function receives an URLInfo object and a Response object containing a response to an HEAD request
	   to the URL specified by the URLInfo
	   It accesses the specified URL and downloads it if the content has one of the acceptable types
	   and is smaller that the maximum size. If the database already contains the content and it has not been modified 
	   we don't download it again
	   If the content is of type text/html the function return an array of links contained in the page
	*/
	public Response access(URLInfo url, Response response) {
		try {
		//DBWrapper database=new DBWrapper(null);
		int resp_code=response.getResponseCode();
		Date pageDate=response.lastModified();
		String contentType=response.header("content-type").split(";")[0].trim();
		int contentLength=response.contentLength()<=0 ? this.maxvalue : response.contentLength();
		Date currentDate=new Date();
		
		// Check if the content is found and has an acceptable size and type

		if(contentLength<=this.maxvalue && contentType!=null && resp_code==200 && this.checkContentType(contentType)) {
			response=this.send_request(url, "GET");
			response.setNewDoc(true);
			System.out.println(url.getNormalized()+": Downloading, Content Type="+contentType);
			/*
			PageKey pagekey=new PageKey(url.getHostName(),url.getFilePath(),url.getPortNo(),url.getProtocol());
			PageValue page;
			page=database.getEntry(pagekey);
			if(page!=null && pageDate.before(page.getDate())) {
				// If the content is already in the database and is not modified since the last download use the database version
				System.out.println(url.getNormalized()+": Not Modified, Content Type="+contentType);
				response.bodyRaw(page.getContent());
				response.setNewDoc(false);
			}else {
				// Else download the content 
				System.out.println(url.getNormalized()+": Downloading, Content Type="+contentType);
				response=this.send_request(url, "GET");
				response.setNewDoc(true);
			}
			*/
		} else {
			System.out.println(url.getNormalized()+" does not meet the requirements for download");
			response.setCode(500);
		}
			
		}catch(Exception e) {
			response.setCode(500);
			//e.printStackTrace();
		}
		return response;
	}
}
