package code.distcrawler.storage;

import java.io.Serializable;

public class UserKey implements Serializable  {
	private String name;
	public UserKey(String in_name) {
		this.name=in_name;
	}
	public final String getValue() {
		return this.name;
	}
	
	public String toString() {
		return "[UserKey: name="+this.name+"]";
	}
}
