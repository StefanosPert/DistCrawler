package code.distcrawler.crawler;

/** (MS1, MS2) Produces a new XPathCrawler.
  */
public class XPathCrawlerFactory {
	public XPathCrawler getCrawler(String monitorURL,int numFiles, int maxFiles) {
		return new XPathCrawler(monitorURL,numFiles,maxFiles);
	}
}
