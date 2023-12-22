package code.distcrawler.storage;

import java.io.Serializable;

public class QueueElement implements Serializable{
	public String URL;
	public String parentURL;
	public QueueElement(String normalURL, String parent) {
		this.URL=normalURL;
		this.parentURL=parent;
	}
	
	public String toString() {
		return "[Queue Element: URL="+this.URL+" with parent="+this.parentURL+"]";
	}
}