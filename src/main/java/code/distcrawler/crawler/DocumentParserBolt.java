package code.distcrawler.crawler;

import static code.distcrawler.crawler.MyUtils.getAbsoluteURL;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
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

public class DocumentParserBolt implements IRichBolt {
	Fields schema = new Fields("URL");
	private OutputCollector collector;
	String executorId = UUID.randomUUID().toString();
	public static AtomicBoolean quit = new AtomicBoolean(false);
	public static AtomicInteger working = new AtomicInteger();

	public DocumentParserBolt() {

	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	// Extract the links from the input document and emit the in the output
	@Override
	public void execute(Tuple input) {
		// Check if the crawler is shutting down
		if (quit.get()) {
			return;
		}

		// Set working status
		working.getAndIncrement();
		try {
			Document document = (Document) input.getObjectByField("DOMDOC");
			URLInfo url = (URLInfo) input.getObjectByField("URL");
			Elements links = document.select("a[href]");
			for (Element elem : links) {
				collector.emit(
						new Values<Object>(new URLInfo(getAbsoluteURL(elem.attr("href"), url), url.getNormalized())));
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
