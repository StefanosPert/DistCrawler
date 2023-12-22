package code.distcrawler.crawler;

import java.io.ByteArrayInputStream;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


import org.jsoup.Jsoup;
import org.jsoup.helper.W3CDom;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Parser;
import org.jsoup.select.Elements;

import code.stormlite.OutputFieldsDeclarer;
import code.stormlite.TopologyContext;
import code.stormlite.bolt.IRichBolt;
import code.stormlite.bolt.OutputCollector;
import code.stormlite.routers.IStreamRouter;
import code.stormlite.tuple.Fields;
import code.stormlite.tuple.Tuple;
import code.stormlite.tuple.Values;
import code.distcrawler.crawler.info.URLInfo;
import code.distcrawler.storage.ChannelKey;
import code.distcrawler.storage.ChannelValue;
import code.distcrawler.storage.DBWrapper;
import code.distcrawler.storage.PageKey;
import code.distcrawler.storage.PageValue;
import code.distcrawler.xpathengine.XPathEngineFactory;
import code.distcrawler.xpathengine.XPathEngineImpl;

import static code.distcrawler.crawler.MyUtils.*;

public class ChannelMatchBolt implements IRichBolt{
	Fields schema=new Fields("URL","DOMDOC");
	private OutputCollector collector;
	private LinkedBlockingQueue<URLInfo> updateQueue;
	private HashMap<String, ChannelValue>  channelMap;
	private String[] channelName;
	private String[] channelXpath;
	DBWrapper db;
	private static XPathCrawler crawler;
	public static AtomicInteger working=new AtomicInteger();
	public static AtomicBoolean quit=new AtomicBoolean(false);
	String executorId = UUID.randomUUID().toString();
	public ChannelMatchBolt(XPathCrawler crawlerInstance) {
		crawler=crawlerInstance;
	}
	public ChannelMatchBolt() {
		
	}
	@Override
	public void prepare(Map<String,String> stormConf, TopologyContext context, OutputCollector collector) {
		this.collector=collector;
		this.channelMap=crawler.getChannelMap();
		Collection<ChannelValue> coll=this.channelMap.values();
		this.channelName=new String[coll.size()];
		this.channelXpath=new String[coll.size()];
		this.updateQueue=crawler.getUpdateQueue();
		this.db=new DBWrapper(null);
		int i=0;
		for(ChannelValue value: coll) {
			this.channelName[i]=value.getName();
			this.channelXpath[i]=value.getXPath();
			i++;
		}
	}
	
	// Given the input document update the database in case the document is first time crawled or if it
	// is modified. Then checks if the document matches any of the channels and update the database accordingly
	// Finally if the document is an "text/html" emit it in the output so it can be further processed.
	@Override
	public void execute(Tuple input) {
		//Check if the crawler is shutting down
		if(quit.get())
			return;
		
		//Set working status
		working.getAndIncrement();
		try {
		URLInfo url=(URLInfo) input.getObjectByField("URL");
		CrawledDocument doc=(CrawledDocument) input.getObjectByField("Doc");
		Date currentDate= new Date();
		
		if(doc.isNew()) {
			PageKey key=new PageKey(url.getHostName(),url.getFilePath(),url.getPortNo(),url.getProtocol());
			PageValue val=new PageValue(url.getHostName(),url.getFilePath(),url.getParent(),
					 currentDate, doc.bodyRaw(),doc.getType() );
			
			try {
			this.updateQueue.put(url);
		    XPathCrawler.pagesAfterStop.getAndIncrement();
			}catch(InterruptedException e) {
				e.printStackTrace();
				System.out.println("Failed to put url="+url.getNormalized()+" into the update list");
			}
			this.db.addEntry(key, val);
		}
		XPathEngineImpl engine=(XPathEngineImpl) XPathEngineFactory.getXPathEngine();
		engine.setXPaths(channelXpath);
		boolean[] channelMatches=null;
		Document document;
		W3CDom w3c=new W3CDom();
		if(doc.getType().contentEquals("text/html")) {
			try {
			document=Jsoup.parse(doc.body(),"",Parser.htmlParser());
			collector.emit(new Values<Object>(url,document));
			channelMatches=engine.evaluate(w3c.fromJsoup(document));
			}catch(Exception e) {
				channelMatches=null;
			}
	
		} else {
			try {
				document=Jsoup.parse(doc.body(),"",Parser.xmlParser());
				channelMatches=engine.evaluate(w3c.fromJsoup(document));
			}catch(Exception e) {
				channelMatches=null;
			}
		}
		for(int j=0; channelMatches!=null &&  j<channelMatches.length; j++) {
			if(channelMatches[j]) {
				System.out.println("  +Adding page: "+url.getNormalized()+" to channel "+channelName[j]);
				ChannelValue value=this.channelMap.get(channelName[j]);
				value.addURL(url.getNormalized());
				ChannelKey key=new ChannelKey(channelName[j]);
				this.db.addEntry(key, value);
			}
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
		this.collector.setRouter(router);
	}
	
	@Override
	public Fields getSchema() {
		return schema;
	}
	
}
