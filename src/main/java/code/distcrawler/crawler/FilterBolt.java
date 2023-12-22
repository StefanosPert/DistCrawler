package code.distcrawler.crawler;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.Set;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import code.stormlite.OutputFieldsDeclarer;
import code.stormlite.TopologyContext;
import code.stormlite.bolt.IRichBolt;
import code.stormlite.bolt.OutputCollector;
import code.stormlite.routers.IStreamRouter;
import code.stormlite.tuple.Fields;
import code.stormlite.tuple.Tuple;
import code.distcrawler.crawler.info.URLInfo;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class FilterBolt implements IRichBolt{
	private static XPathCrawler crawler;
	public static AtomicInteger working=new AtomicInteger();
	public static AtomicBoolean quit=new AtomicBoolean(false);
	private ConcurrentHashMap<String,Date> visitedURLs;
	private BlockingQueue<CrawlerElement> frontier;
	
	Fields schema=new Fields();
	String executorId = UUID.randomUUID().toString();
	
	public FilterBolt(XPathCrawler crawlerInstance) {
		crawler=crawlerInstance;
	}
	public FilterBolt() {
		
	}
	@Override
	public void prepare(Map<String,String> stormConf, TopologyContext context,OutputCollector collector) {
		this.visitedURLs=crawler.getURLMap();
		this.frontier=crawler.getFrontier();
	}
	
	public boolean isVisited(String normalURL) {
		Map<String,AttributeValue> result=XPathCrawler.awsHandler.getDynamoDBItem("visitedURLs", "URL", normalURL);
		if(result!=null && result.keySet().size()>0) {
			if(Long.valueOf(result.get("date").s())>XPathCrawler.startingTime) {
				return true;
			}
		} 
		return false;
		
	}
	// Check if the input URL is already visited, if not put it in the frontier
	@Override
	public void execute(Tuple input) {
		//Check if the crawler is shutting down
		if(quit.get())
			return;
		
		//Set working status
		working.getAndIncrement();
		try {
		URLInfo url=(URLInfo) input.getObjectByField("URL");
		String[] part=url.getFilePath().split("/");
		if(!this.isVisited(url.getNormalized()) && part.length<4) {
			Date currentDate=new Date();
			CrawlerElement newURL=new CrawlerElement(0,currentDate.getTime(),url);
			
			if(XPathCrawler.awsHandler.putItemInTable("visitedURLs", "URL", url.getNormalized(), "date", String.valueOf(currentDate.getTime())))
			 XPathCrawler.awsHandler.sendURL(newURL);
		}

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
		
	}
	
	@Override
	public Fields getSchema() {
		return schema;
	}
}
