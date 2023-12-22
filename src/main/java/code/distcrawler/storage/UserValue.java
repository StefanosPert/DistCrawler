package code.distcrawler.storage;
import java.io.Serializable;
import java.util.HashSet;

public class UserValue implements Serializable
{
	private String name;
	private String password;
	private String firstName;
	private String lastName;
	private HashSet<String> channels;
	public UserValue(String in_name, String in_password, String in_first, String in_last) {
		this.name=in_name;
		this.password=in_password;
		this.firstName=in_first;
		this.lastName=in_last;
		this.channels=new HashSet<String>();
	}
	public final String getName() {
		return this.name;
	}
	public final String getPass() {
		return this.password;
	}
	public final String getFirstName() {
		return this.firstName;
	}
	public final String getLastName() {
		return this.lastName;
	}
	
	public void setChannelSet(HashSet<String> newSet) {
		this.channels=newSet;
	}
	public void addChannel(String chan) {
		this.channels.add(chan);
	}
	public void removeChannel(String chan) {
		this.channels.remove(chan);
	}
	public final HashSet<String> getChannelSet(){
		return this.channels;
	}
	
	public String toString() {
		return "[UserValue: name="+this.name+" password="+this.password+"]";
	}
}
