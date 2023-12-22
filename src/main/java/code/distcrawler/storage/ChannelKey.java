package code.distcrawler.storage;

import java.io.Serializable;

public class ChannelKey implements Serializable {
	private String channelName;
	
	public ChannelKey(String name) {
		this.channelName=name;
	}
	
	public final String getName() {
		return this.channelName;
	}
	
	public String toString() {
		return "[ChannelKey: ChannelName="+this.channelName+"]";
	}
}
