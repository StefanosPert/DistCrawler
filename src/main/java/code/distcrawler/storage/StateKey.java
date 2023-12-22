package code.distcrawler.storage;

import java.io.Serializable;

public class StateKey implements Serializable{
	private String checkpoint;
	public StateKey(String checkName) {
		this.checkpoint=checkName;
	}
	public final String getCheckpoint() {
		return this.checkpoint;
	}
	public String toString() {
		return "[Checkpoint: name="+this.checkpoint+"]";
	}
}

