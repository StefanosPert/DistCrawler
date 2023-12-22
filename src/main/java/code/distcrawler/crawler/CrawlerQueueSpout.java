package code.distcrawler.crawler;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import code.stormlite.OutputFieldsDeclarer;
import code.stormlite.TopologyContext;
import code.stormlite.routers.IStreamRouter;
import code.stormlite.spout.IRichSpout;
import code.stormlite.spout.SpoutOutputCollector;
import code.stormlite.tuple.Fields;
import code.stormlite.tuple.Values;
import code.distcrawler.crawler.info.URLInfo;

public class CrawlerQueueSpout implements IRichSpout{
	private static XPathCrawler crawler;
	private BlockingQueue<CrawlerElement> frontier;
	public static AtomicInteger working=new AtomicInteger();
	public static AtomicBoolean quit=new AtomicBoolean(false);
	private SpoutOutputCollector collector;
	private AWSHandler aws=null;
    private String executorId = UUID.randomUUID().toString();
    private static AtomicInteger failedPolls=new AtomicInteger();
	private IStreamRouter outputRouter;
	public CrawlerQueueSpout(XPathCrawler crawlerInstance) {
		crawler=crawlerInstance;
	}
	public CrawlerQueueSpout() {
		
	}
	@Override
	public void open(Map conf,TopologyContext context, SpoutOutputCollector collector) {
		this.collector=collector;
		this.frontier=crawler.getFrontier();
		this.aws=XPathCrawler.awsHandler;
	}
	
	@Override
	public void close() {
		
	}
	
	// Gets the next URL from the frontier and emits it, so it can be processed by the bolts
	@Override
	public void nextTuple() {
	   //Check if the crawler is shutting down
	   if(quit.get())
		   return;
	   
	   //Set working status 
	   working.getAndIncrement();
	   try {
		   CrawlerElement nextElement=this.aws.getURL();
		   
		   if(nextElement!=null) {
		   failedPolls.getAndSet(0);
		   URLInfo  nextUrl=new URLInfo(nextElement.normUrl,nextElement.parent);
		   //Date currentTime=new Date();
		   /*
		   if(nextElement.WaitUntil()-currentTime.getTime()>0) {
			  // If we need to wait D seconds to access the host due to crawl delay, sleep for D seconds  
			  try {
			   Thread.sleep(nextElement.WaitUntil()-currentTime.getTime());
			  }catch(InterruptedException e) {
				  e.printStackTrace();
			  }
		   }
		   */
		   // Emit the value of the next URL to be crawled
		   this.collector.emit(new Values<Object>(nextUrl));
	   } else {
		   // If queue is empty sleep 1 second. If this is repeated for a set amount of 
		   // time call the shutdown function and close the shut down the crawler
		   if(failedPolls.getAndIncrement()>600) {
			   System.out.println("Nothing else to crawl");
			   working.decrementAndGet();
			   crawler.shutdown();
		   }else {
			   Thread.sleep(1000);
		   }
	   }
	  
	   }catch(InterruptedException e) {
		   
	   }
	   finally {
		   // Update working status
		   working.getAndDecrement();
		   Thread.yield();
	   }
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("URL"));
	}
	
	@Override
	public String getExecutorId() {
		return executorId;
	}
	
	@Override
	public void setRouter(IStreamRouter router) {
		this.collector.setRouter(router);
	}
}
