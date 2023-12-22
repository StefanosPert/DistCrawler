package code.distcrawler.crawler;

import static code.distcrawler.crawler.MyUtils.getAbsoluteURL;
import static code.distcrawler.crawler.MyUtils.getNormalizedUrl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import code.stormlite.Config;
import code.stormlite.LocalCluster;
import code.stormlite.Topology;
import code.stormlite.TopologyBuilder;
import code.stormlite.tuple.Values;
import code.distcrawler.crawler.info.RobotsTxtInfo;
import code.distcrawler.crawler.info.URLInfo;
import code.distcrawler.storage.ChannelKey;
import code.distcrawler.storage.ChannelValue;
import code.distcrawler.storage.DBWrapper;
import code.distcrawler.storage.PageKey;
import code.distcrawler.storage.PageValue;
import code.distcrawler.storage.QueueElement;
import code.distcrawler.storage.StateKey;
import code.distcrawler.storage.StateValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;


import static code.distcrawler.crawler.MyUtils.*;

/**
 * (MS1, MS2) The main class of the crawler. We can run the crawler by the
 * following command mvn exec:java@Crawler -Dexec.args="Start_URL
 * /path/to/local/storage MAX_SIZE_OF_PAGE NUM_NEW_PAGES_TO_UPDATE
 * MAXIMUM_NUM_PAGES_DOWNLOAD" When Start_URL=continue the crawler start from
 * checkpoint stored in /path/to/local/storage/StateDB directory in a BerkeleyDB
 * database form
 */
public class XPathCrawler {
	private LinkedBlockingQueue<CrawlerElement> frontier;
	public static long pos = 0;
	public static AtomicInteger pagesAfterStop = new AtomicInteger();
	private LinkedBlockingQueue<URLInfo> urlStoredLocally;
	private CrawlerHttpClient client;
	public static final double DEFAULT_DELAY = 0.1;
	private int maxBytes = 1000000;
	private RobotsTxtInfo robots;
	static String storageDir;
	private ConcurrentHashMap<String, Host> visitedHosts;
	private ConcurrentHashMap<String, Date> visitedURLs;
	private String monitorURL;
	private boolean initialized = false;
	private int numFiles;
	private int maxFiles;
	private AtomicInteger crawledPages;
	private HashMap<String, ChannelValue> channelMap;
	private Thread main_thread;
	public static AtomicBoolean quit = new AtomicBoolean(false);
	public static long startingTime=0;
	public static long savedPages=0;
	public static int updateCalls=0;
	public static int id=0;
	public static int maxCrawlerHeads=1;
	private static String crawlerID="crawler0";

	
	private static final String CRAWLER_QUEUE_SPOUT = "CRAWLER_QUEUE_SPOUT";
	private static final String CRAWLER_BOLT = "CRAWLER_BOLT";
	private static final String MATCH_BOLT = "MATCH_BOLT";
	private static final String DOCUMENT_PARSER = "DOCUMENT_PARSER";
	private static final String FILTER_BOLT = "FILTER_BOLT";

	public static AWSHandler awsHandler;
	
	private static BodyProcessor bodyProcessor;

	public static void main(String args[]) {
		Logger.getRootLogger().removeAllAppenders();
		// initial awshandler
		// clear bucket and dynamodb before start
		crawlerID = args[6];
		String bucketName = args[7];
	    String[] idCommand=crawlerID.split(":");
        id=Integer.valueOf(idCommand[0]);
        maxCrawlerHeads=Integer.valueOf(idCommand[1]);
        crawlerID="crawler"+id;
        
		awsHandler = new AWSHandler(crawlerID, bucketName,id,maxCrawlerHeads);
		bodyProcessor = new BodyProcessor();
	
		// Read the input arguments
		if (args.length < 3 || args.length>8) {
			System.err.println("You need to provide the URL of the Web Page to start crawling, "
					+ "the path of the directory of the Berkeley database, the maximum size in megabytes for retrieved files,"
					+ "the number of file between each update to S3,the maximum number of files to crawl,"
					+ " the hostname for monitoring, the crawler ID, S3 bucket name.");
			System.exit(1);
		}

		storageDir = args[1];
		File dbDir = new File(storageDir + "/BerkeleyDB");
		File stateDir = new File(storageDir + "/StateDB");
		dbDir.mkdir();
		stateDir.mkdir();

		new DBWrapper(storageDir + "/BerkeleyDB");
		String urlForMonitor = "cis";

		String startingURL;
		if (args[0].contentEquals("continue")) {
			startingURL = null;
		} else {
			startingURL = args[0];
		}

		// Initialize a instance of the crawler
		XPathCrawlerFactory crawlerFact = new XPathCrawlerFactory();
		int number_of_files = 10;
		if (args.length >= 4) {
			number_of_files = Integer.valueOf(args[3]);
		}
		int max_files = 100;
		if (args.length >= 5) {
			max_files = Integer.valueOf(args[4]);
		}
		if (args.length >= 6) {
			urlForMonitor = args[5];
		}
		XPathCrawler crawler = crawlerFact.getCrawler(urlForMonitor, number_of_files, max_files);
		try {
			// Run the crawler from the startingURL with the appropriate value for maximum
			// page size
			crawler.run(startingURL, Integer.valueOf(args[2]));
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Finishing Crawling");
		System.exit(0);

	}

	public XPathCrawler(String url, int periodicNum, int maxNum) {
		this.monitorURL = url;
		this.numFiles = periodicNum;
		this.maxFiles = maxNum;
	}

	// For the input ArrayList with disallow links parse from the robots.txt, check
	// if the crawler can crawl the input URL
	public boolean notAllowedURL(ArrayList<String> links, URLInfo url) {
		String path = unencodeOctets(url.getFilePath());
		for (String link : links) {
			link = unencodeOctets(link);
			if (!link.contentEquals("") && path.startsWith(link)) {
				return true;
			}
		}
		return false;
	}

	public LinkedBlockingQueue<CrawlerElement> getFrontier() {
		return this.frontier;
	}

	public LinkedBlockingQueue<URLInfo> getUpdateQueue() {
		return this.urlStoredLocally;
	}

	public ConcurrentHashMap<String, Host> getHostMap() {
		return this.visitedHosts;
	}

	public ConcurrentHashMap<String, Date> getURLMap() {
		return this.visitedURLs;
	}

	public RobotsTxtInfo getRobots() {
		return this.robots;
	}

	public CrawlerHttpClient getClient() {
		return this.client;
	}

	public int getMaxFiles() {
		return this.maxFiles;
	}

	public int getNumFiles() {
		return this.numFiles;
	}

	public HashMap<String, ChannelValue> getChannelMap() {
		return this.channelMap;
	}

	public AtomicInteger getCrawledPages() {
		return this.crawledPages;
	}

	public synchronized static void update(XPathCrawler crawler) {
		LinkedBlockingQueue<URLInfo> queue = crawler.getUpdateQueue();
		DBWrapper db = new DBWrapper(null);
		String contentFile = storageDir + "/" + AWSHandler.getTimeStamp() + "contentFile";
		String pageRankFile = storageDir + "/" + AWSHandler.getTimeStamp() + "pageRankFile";
//		String metadataFile = storageDir + "/" + AWSHandler.getTimeStamp() + metaFile";
		
        long storedFiles=0;
        updateCalls++;



		long contentSize = 0;
		try {
			FileOutputStream contentWriter = new FileOutputStream(contentFile);
			FileOutputStream pageRankFileWriter = new FileOutputStream(pageRankFile);
//			BufferedWriter metaWriter = new BufferedWriter(new FileWriter(metadataFile));

			ObjectMapper mapper = new ObjectMapper();
			int filesSoFar=crawler.getNumFiles();
			for (int i = 0; i < filesSoFar && !queue.isEmpty(); i++) {
				try {
				URLInfo url = queue.poll();
				//url.getParent();
				if (url != null) {
					System.out.println("URL stored=" + url.getNormalized());
					PageKey pagekey = new PageKey(url.getHostName(), url.getFilePath(), url.getPortNo(),
							url.getProtocol());
					PageValue page = db.getEntry(pagekey);
					System.out.print("Parsing ");
					if (page != null) {
					
						byte[] byteBuff = page.getContent();
						System.out.print("results: ");
						Document document = Jsoup.parse(new String(byteBuff));
					
						//************************
						//Below is for pageRank string builder
						//************************
						Elements links = document.select("a[href]");
						List<String> outlinks = new ArrayList<String>();
						for (Element elem : links) {
							String out = getAbsoluteURL(elem.attr("href"), url);
							out = getNormalizedUrl(out);
							if(out.length() == 0) {
								continue;
							}
							out = AWSHandler.MD5Hash(out);
							outlinks.add(out);
						}
						StringBuilder pageRankFileBuilder = new StringBuilder();
						pageRankFileBuilder.append(AWSHandler.MD5Hash(url.getNormalized()));
						pageRankFileBuilder.append("::1::");
						for(int j = 0; j < outlinks.size() - 1; j++) {
							pageRankFileBuilder.append(outlinks.get(j) + " ");
						}
						if((outlinks.size() - 1)>=0) {
							pageRankFileBuilder.append(outlinks.get(outlinks.size() - 1));
						}
						pageRankFileBuilder.append("\n");
						byte[] PRoutputBuff = pageRankFileBuilder.toString().getBytes(StandardCharsets.UTF_8);

						try {
							pageRankFileWriter.write(PRoutputBuff);
							pageRankFileWriter.flush();
						} catch (IOException e) {
							System.out.println("Unable to write to contnet File");
							continue;
						}
						
						//************************
						//Below is for contentFile string builder
						//************************
						
						// parse html locally
						String hashedURL = AWSHandler.MD5Hash(url.getNormalized());

						// judge duplication
						if (awsHandler.containsUrl(hashedURL)) { continue; }

						String rawbodyTitle = document.title();
						
						if(rawbodyTitle.length() == 0 || rawbodyTitle.indexOf(' ') == -1) { continue; }

						// below is get description and keywords in meta
						Elements metaTags = document.getElementsByTag("meta");
						String rawdescription = "";
						String keywords = "";

						StringBuilder coreBody = new StringBuilder();
						for (Element metaTag : metaTags) {
							String content = metaTag.attr("content");
							String name = metaTag.attr("name");
							if ("description".equals(name)) {
								rawdescription = content;
							} else if (name.endsWith("description") && rawdescription.length() == 0) {
								rawdescription = content;
							} else if (name.equals("keywords")) {
								keywords = content;
							}
						}
						
						String bodyTitle = BodyProcessor.process(rawbodyTitle);
						String description = BodyProcessor.process(rawdescription);
						keywords = BodyProcessor.process(keywords);
						
						if(bodyTitle.length() > 0) { coreBody.append(bodyTitle + " "); }
						if(description.length() > 0) { coreBody.append(description + " "); }
						if(keywords.length() > 0) { coreBody.append(keywords + " "); }
						
						Elements hTags = document.select("h1, h2, h3, h4, h5, h6");
						for(Element hTag: hTags) {
							if(hTag.hasText() && hTag.text().length() > 0) {
								coreBody.append(BodyProcessor.process(hTag.text()) + " ");
							}
						}
						
						String coreBodyStr = coreBody.toString();
						// then trim the coreBodyStr
						int bodyLen = 0;
						String[] coreBodyArr = coreBodyStr.split(" ");
						coreBody = new StringBuilder();
						for(String word: coreBodyArr) {
							word = word.trim();
							if(word.length() > 0) {
								coreBody.append(word + " ");
								bodyLen++;
							}
						}
						coreBodyStr = coreBody.toString();
						
						if(rawdescription.equals("")) { rawdescription = "No description for this page"; }
						
						awsHandler.addToDDB(hashedURL, url.getNormalized(), String.valueOf(bodyLen),
								rawbodyTitle, rawdescription);

						StringBuilder sb = new StringBuilder();
						sb.append(hashedURL);
						sb.append("####");
						sb.append(coreBodyStr);
						sb.append("\n");

						byte[] outputBuff = sb.toString().getBytes(StandardCharsets.UTF_8);

						try {
							contentWriter.write(outputBuff);
							contentWriter.flush();
						} catch (IOException e) {
							System.out.println("Unable to write to contnet File");
							continue;
						}
						pos = pos + contentSize;
						
						storedFiles++;
						db.deleteEntry(pagekey);

					}
				}
				}catch(NullPointerException e) {
					e.printStackTrace();
				}
			}
			contentWriter.close();
			pageRankFileWriter.close();

			awsHandler.putContentFileIntoS3(contentFile);
			awsHandler.putPageRankIntoS3(pageRankFile);
			
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Unable to open storage files, was not able to update");
		}
		XPathCrawler.savedPages=XPathCrawler.savedPages+storedFiles;
		System.out.println("**********************\nSuccesfully updated remote DB "+updateCalls+" Total files stored="+XPathCrawler.savedPages+"\n************************");
		System.out.flush();
	}

	// Wait for all the Spouts and Bolts of the crawler to terminate what they are
	// currently
	// doing and then send a interrupt signal to the main thread in order to
	// terminate the crawler
	public void shutdown() {
		if (!quit.getAndSet(true)) {
			long start_shutdown = (new Date()).getTime();
			System.out.println("Waiting for all tasks to terminate before shutdown");
			CrawlerQueueSpout.quit.getAndSet(true);
			while (CrawlerQueueSpout.working.get() > 0) {
				if ((new Date()).getTime() - start_shutdown > 20000) {
					XPathCrawler.update(this);
					System.out.println("Force termination");
					System.exit(0);
				}
				Thread.yield();
			}
			System.out.println("Crawler Queue Spouts are done");
			CrawlerBolt.quit.getAndSet(false);
			while (CrawlerBolt.working.get() > 0) {
				if ((new Date()).getTime() - start_shutdown > 20000) {
					XPathCrawler.update(this);
					System.out.println("Force termination");
					System.exit(0);
				}
				Thread.yield();
			}
			System.out.println("Crawler Bolts are done");
			ChannelMatchBolt.quit.getAndSet(false);
			while (ChannelMatchBolt.working.get() > 0) {
				if ((new Date()).getTime() - start_shutdown > 20000) {
					XPathCrawler.update(this);
					System.out.println("Force termination");
					System.exit(0);
				}
				Thread.yield();
			}
			System.out.println("Crawler Match Bolts are done");
			DocumentParserBolt.quit.getAndSet(false);
			while (DocumentParserBolt.working.get() > 0) {
				if ((new Date()).getTime() - start_shutdown > 20000) {
					XPathCrawler.update(this);
					System.out.println("Force termination");
					System.exit(0);
				}
				Thread.yield();
			}
			System.out.println("Document Parser Bolts are done");
			FilterBolt.quit.getAndSet(false);
			while (FilterBolt.working.get() > 0) {
				if ((new Date()).getTime() - start_shutdown > 20000) {
					XPathCrawler.update(this);
					System.out.println("Force termination");
					System.exit(0);
				}
				Thread.yield();
			}
			System.out.println("Filter Bolts are done");

			synchronized (this.main_thread) {

				this.main_thread.interrupt();

			}
		}
	}

	// Initialize the crawler data structures and the Http Client
	public void init(String startingURL, Integer maxvalue) {
		startingTime=(new Date()).getTime();

		this.frontier = new LinkedBlockingQueue<CrawlerElement>();
		this.visitedHosts = new ConcurrentHashMap<String, Host>();
		this.urlStoredLocally = new LinkedBlockingQueue<URLInfo>();
		this.visitedURLs = new ConcurrentHashMap<String, Date>();
		this.robots = new RobotsTxtInfo();
		this.crawledPages = new AtomicInteger();
		this.channelMap = new HashMap<String, ChannelValue>();

		SimpleDateFormat format = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z");
		DBWrapper db = new DBWrapper(null);
		ArrayList<ChannelValue> channels = db.getChannels();
		for (ChannelValue elem : channels) {
			elem.resetURL();
			this.channelMap.put(elem.getName(), elem);
			ChannelKey channelkey = new ChannelKey(elem.getName());
			db.addEntry(channelkey, elem);
		}
		// Add the starting URL in the frontier
		  if(startingURL!=null) {
			  if(id==0) {
				  awsHandler.putItemInTable("StartingTime", "crawlerId", "0", "date", String.valueOf(startingTime));
			  }
			  awsHandler.initQueue(false, id);
			  if(id==0) {
				try {
				BufferedReader  reader = new BufferedReader(new FileReader(startingURL));
				String line = reader.readLine();
				while (line != null) {
						line=line.trim();
						URLInfo newUrl=new URLInfo(line,"none");
						System.out.println("Adding URL: "+newUrl.getNormalized()+" into the frontier");
						CrawlerElement URLelement=new CrawlerElement(0,(new Date()).getTime(),newUrl);
						awsHandler.sendURL(URLelement);

						line = reader.readLine();
					}
				}catch(IOException e) {
					e.printStackTrace();
					System.out.println("Failed to initialize frontier");
					System.exit(1);
				}
			  }
			

		  } else {
			  awsHandler.initQueue(true,id);
		  }
		  Map<String,AttributeValue> result=XPathCrawler.awsHandler.getDynamoDBItem("StartingTime", "crawlerId", "0");
		  if(result!=null && result.keySet().size()>0) {
				startingTime=Long.valueOf(result.get("date").s());
		  } 
		  else {
			  System.out.println("Was not able to recover starting time");
			  System.exit(1);
		  }

		this.maxBytes = maxvalue * 1000000;
		// Initialize Http Client
		this.client = new CrawlerHttpClient(this.monitorURL, this.maxBytes);
	}

	/*
	 * Runs the crawler starting from URL specified in startingURL with maximum size
	 * for files to download specified in maxvalue (in MB) The frontier is
	 * implemented with a priority queue where each element has the time in
	 * milliseconds after which it can be accessed again (the time depends on the
	 * crawl delay). Between any two elements the highest priority has the element
	 * that can be accessed sooner It consists of one spout that pull URL out of the
	 * frontier and four bolt that access the URLs and update the database and the
	 * frontier accordingly
	 */
	public void run(String startingURL, Integer maxvalue) throws Exception {

		this.main_thread = Thread.currentThread();
		if (!this.initialized) {
			this.init(startingURL, maxvalue);
			this.initialized = true;
		}

		Config config = new Config();

		// Initialize the spout and the bolts
		CrawlerQueueSpout spout = new CrawlerQueueSpout(this);
		CrawlerBolt crawlerBolt = new CrawlerBolt(this);
		ChannelMatchBolt matchBolt = new ChannelMatchBolt(this);
		DocumentParserBolt docBolt = new DocumentParserBolt();
		FilterBolt filterBolt = new FilterBolt(this);

		TopologyBuilder builder = new TopologyBuilder();

		// Set the Topology
		builder.setSpout(CRAWLER_QUEUE_SPOUT, spout, 8);
		builder.setBolt(CRAWLER_BOLT, crawlerBolt, 8).shuffleGrouping(CRAWLER_QUEUE_SPOUT);
		builder.setBolt(MATCH_BOLT, matchBolt, 8).shuffleGrouping(CRAWLER_BOLT);
		builder.setBolt(DOCUMENT_PARSER, docBolt, 8).shuffleGrouping(MATCH_BOLT);
		builder.setBolt(FILTER_BOLT, filterBolt, 8).shuffleGrouping(DOCUMENT_PARSER);

		LocalCluster cluster = new LocalCluster();
		Topology topo = builder.createTopology();
		ObjectMapper mapper = new ObjectMapper();
		try {
			String str = mapper.writeValueAsString(topo);

			System.out.println("The StormLite topology is:\n" + str);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Start running the topology
		cluster.submitTopology("crawler", config, builder.createTopology());
		cluster.submitTopology("crawler", config, topo);
		try {
			synchronized (this.main_thread) {
				if (!this.main_thread.isInterrupted()) {
					// Wait to be interrupted in order to stop the crawler
					this.main_thread.wait();
				}
			}
		} catch (InterruptedException e) {
			cluster.killTopology("crawler");
			cluster.shutdown();
			XPathCrawler.update(this);
			System.out.println("Crawler shutting down");
			System.exit(0);
		}
		System.exit(0);
	}
}
