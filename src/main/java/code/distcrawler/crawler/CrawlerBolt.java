package code.distcrawler.crawler;

import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import code.stormlite.OutputFieldsDeclarer;
import code.stormlite.TopologyContext;
import code.stormlite.bolt.IRichBolt;
import code.stormlite.bolt.OutputCollector;
import code.stormlite.routers.IStreamRouter;
import code.stormlite.tuple.Fields;
import code.stormlite.tuple.Tuple;
import code.stormlite.tuple.Values;
import code.distcrawler.crawler.info.RobotsTxtInfo;
import code.distcrawler.crawler.info.URLInfo;

public class CrawlerBolt implements IRichBolt{
	private static XPathCrawler crawler;
	private BlockingQueue<CrawlerElement> frontier;
	private LinkedBlockingQueue<URLInfo> updateQueue;
	Fields schema=new Fields("URL","Doc");
	public static AtomicInteger working=new AtomicInteger();
	public static AtomicBoolean quit=new AtomicBoolean(false);
	String executorId = UUID.randomUUID().toString();
	private OutputCollector collector;
	private AtomicInteger crawledFiles;
	private ConcurrentHashMap<String,Host> visitedHosts=null;
	private RobotsTxtInfo robots=null;
	private CrawlerHttpClient client=null;
	public CrawlerBolt(XPathCrawler crawlerInstance) {
		crawler=crawlerInstance;
	}
	public CrawlerBolt() {
		
	}
	@Override
	public void prepare(Map<String,String> stormConf, TopologyContext context, OutputCollector collector) {
		this.collector=collector;
		this.visitedHosts=crawler.getHostMap();
		
		this.client=crawler.getClient();
		this.robots=crawler.getRobots();
		this.frontier=crawler.getFrontier();
		this.crawledFiles=crawler.getCrawledPages();
		this.updateQueue=crawler.getUpdateQueue();
	}
	
	// Access the input URL to retrieve the page either by downloading it or from the database
	// in case we have already visited the URL. Must respect robots text delays during the process.
	// We respect the crawl delay between consecutive HEAD request to the same server for different URLs
	@Override
	public void execute(Tuple input) {
		//Check if the crawler is shutting down
		if(quit.get())
			return;
		
		//Set working status
		working.getAndIncrement();
		try {
			if(this.crawledFiles.get()%100==0) {
				System.out.println("****************************\n \n Stored Files="+XPathCrawler.savedPages
									+"\n *****************************");
			}
			if(this.crawledFiles.get()<crawler.getMaxFiles()) {
			  boolean update=false;
			 // System.out.print(" I am preparing for the redirect");
			  synchronized(XPathCrawler.pagesAfterStop) {
				  if(XPathCrawler.pagesAfterStop.get()>=crawler.getNumFiles()) {
					  XPathCrawler.pagesAfterStop.addAndGet(-crawler.getNumFiles());
					  update=true;
				  }
			  }
			  if(update) {
				  XPathCrawler.update(crawler);
			  }
			  URLInfo currURL=(URLInfo) input.getObjectByField("URL");
			  Date currentTime;
		  
			  int redirect_count=0;
			  Response response=null;
			  ArrayList<String> disallowedLinks=null;
			  int respCode=300;
			  // Check for redirect ( maximum 10 redirects)
			  while((respCode>=300 && respCode<400) && redirect_count<10) {
				 // System.out.print(" I am in the redirect");
				  redirect_count++;
				  String norm_host=currURL.getHostName()+":"+currURL.getPortNo();
				  if(!this.visitedHosts.containsKey(norm_host)) {
					  this.client.updateRobotsTxtInfo(this.robots, currURL, "cis455crawler");
				  }
				  
				  disallowedLinks=this.robots.getDisallowedLinks(norm_host);
				  if(disallowedLinks!=null && crawler.notAllowedURL(disallowedLinks,currURL)) {
					  // If robots.txt doesn't allow for the crawler to access the URL then proceed to the next URL of the frontier
					  System.out.println("We are not allowed to crawl "+currURL.getHostName()+currURL.getFilePath());
					  break;
				  }
				  synchronized(this.visitedHosts) {
					  int numVisits=0;
					  if( visitedHosts.containsKey(norm_host)) {
						  //System.out.print(" I am in the visited hosts");
						  //System.out.println("I have visited again "+norm_host);
						  double delay=XPathCrawler.DEFAULT_DELAY;
						  if(this.robots.crawlContainAgent(norm_host) ) {
							  delay=this.robots.getCrawlDelay(norm_host);
						  }
						  if(delay>2)// Safety for pages with unreasonable crawl delay
							  delay=2;
						  Host host=this.visitedHosts.get(norm_host);
						  Date lastVisited=host.date;
						  numVisits=host.visits;
						  if(numVisits>1000) {
							  working.getAndDecrement();
							  return;
						  }
						  currentTime=new Date();
						  if(currentTime.getTime()<lastVisited.getTime()+delay*1000) {
						  // If host is visited before and the appropriate delay hasn't pass push the URL back in 
						  // the frontier and proceed without accessing it
							  if((lastVisited.getTime()+Math.round(delay*1000)-currentTime.getTime())<=500) {
								  //System.out.print("Sleeping");
								  Thread.sleep(lastVisited.getTime()+Math.round(delay*1000)-currentTime.getTime());
								  //System.out.println(" I am awake");
							  } else {
								  XPathCrawler.awsHandler.sendURL(new CrawlerElement(0,currentTime.getTime(),currURL));
								  response=null;
								  working.getAndDecrement();
								  //System.out.println("Pushing back in the queue");
								  return;
							  }

						  }
						  
					  }
					  this.visitedHosts.put(currURL.getHostName()+":"+currURL.getPortNo(), new Host(new Date(),numVisits+1));
				  }
				  //Send a HEAD request to the URL that is currently crawled
				  //System.out.print(" I am sending response");
				  response=this.client.checkRedirect(currURL,"HEAD");
				  
				  
				  respCode=response.getResponseCode();
				  if(!(respCode>=300 && respCode<400)) {
					  //System.out.println("I got redirected");
					  crawledFiles.getAndIncrement();
				  }
				  currURL=response.getURL();
			  }
			
			  if(response!=null && response.getResponseCode()==200) {
				  // If the resource of the URL is available get the content and the extracted links

				   response=this.client.access(currURL,response);
				   if(response.getResponseCode()==200) {
					   CrawledDocument doc=new CrawledDocument(response.bodyRaw());
					   doc.setNew(response.isNewDoc());
					   doc.setType(response.getContentType());
					   // If the document retrieved emit the URL and the document object containing it content and information
					   collector.emit(new Values<Object>(currURL,doc));
				   }
				  } 
			} else {
				// If the crawler have passed the maximum allowed files to crawl shut down the crawler
				System.out.println("Crawler Bolt calling shutdown because we have crawled "+crawler.getMaxFiles()+" files");
				working.getAndDecrement();
				crawler.shutdown();
			}
		  }catch(Exception e) {
			  e.printStackTrace();
		  } finally {
			  // Update working status
			  working.getAndDecrement();
		  }
	}
	
	@Override
	public void cleanup() {
		
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(schema);
	}
	
	@Override
	public String getExecutorId() {
		return this.executorId;
	}
	
	@Override
	public void setRouter(IStreamRouter router) {
		this.collector.setRouter(router);
	}
	
	@Override
	public Fields getSchema() {
		return schema;
	}
}
