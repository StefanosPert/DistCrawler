package code.distcrawler.crawler.info;

/** (MS1, MS2) Holds information about a URL.
  */
public class URLInfo {
	private String hostName;
	private int portNo;
	private String filePath;
	private String protocolType="HTTP";
	private String parentLink="none";
	private int defaultPort=80;
	
	/**
	 * Constructor called with raw URL as input - parses URL to obtain host name and file path
	 */
	public boolean isHTTPS() {
		if(this.protocolType.contentEquals("HTTPS")) {
			return true;
		}
		return false;
	}
	
	public String getProtocol() {
		return this.protocolType;
	}
	public String getParent() {
		return this.parentLink;
	}
	public String getNormalized() {
		return this.getProtocol().toLowerCase()+"://"+this.getHostName()+":"+this.getPortNo()+this.getFilePath();
	}
	public URLInfo(String docURL,String parent){
		if(docURL == null || docURL.equals(""))
			return;
		this.parentLink=parent;
		docURL = docURL.trim();
		
		if(!((docURL.startsWith("http://") && docURL.length()>7) || (docURL.startsWith("http://") || docURL.length() > 8))) {
			//System.out.println("Returning empty handed");
			return;}
		// Stripping off 'http://'
		if(docURL.startsWith("http://")) {
			//System.out.println("Setting protocol to http");
			docURL = docURL.substring(7);
			this.protocolType="HTTP";
		}else if(docURL.startsWith("https://")){
			//System.out.println("Setting protocol to https");
			docURL=docURL.substring(8);
			this.protocolType="HTTPS";
			this.defaultPort=443;
		}
		/*If starting with 'www.' , stripping that off too
		if(docURL.startsWith("www."))
			docURL = docURL.substring(4);*/
		int i = 0;
		while(i < docURL.length()){
			char c = docURL.charAt(i);
			if(c == '/')
				break;
			i++;
		}
		String address = docURL.substring(0,i);
		if(i == docURL.length())
			filePath = "/";
		else
			filePath = docURL.substring(i); //starts with '/'
		if(address.equals("/") || address.equals(""))
			return;
		if(address.indexOf(':') != -1){
			String[] comp = address.split(":",2);
			hostName = comp[0].trim();
			try{
				portNo = Integer.parseInt(comp[1].trim());
			}catch(NumberFormatException nfe){
				portNo = this.defaultPort;
			}
		}else{
			hostName = address;
			portNo = this.defaultPort;
		}
	}
	
	/*
	public URLInfo(String hostName, String filePath){
		this.hostName = hostName;
		this.filePath = filePath;
		this.portNo = this.defaultPort;
	}
	*/
	public URLInfo(String hostName,int portNo,String filePath){
		this.hostName = hostName;
		this.portNo = portNo;
		this.filePath = filePath;
	}
	
	public String getHostName(){
		return hostName;
	}
	
	public void setHostName(String s){
		hostName = s;
	}
	
	public int getPortNo(){
		return portNo;
	}
	
	public void setPortNo(int p){
		portNo = p;
	}
	
	public String getFilePath(){
		return filePath;
	}
	
	public void setFilePath(String fp){
		filePath = fp;
	}
	
}
