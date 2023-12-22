package code.distcrawler.xpathengine;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.helpers.DefaultHandler;

/** (MS2) Implements XPathEngine to handle XPaths.
  */
public class XPathEngineImpl implements XPathEngine {
  private ArrayList<String> paths=new ArrayList<String>();
  private Pattern namepat=Pattern.compile("^([a-zA-Z\\_]+[a-zA-Z0-9\\_\\.\\-]*)(\\[|\\/|$)");
  private Pattern textPat=Pattern.compile("^text\\s*\\(\\s*\\)\\s*=\\s*\"(.*)\"\\s*$");
  private Pattern attPat=Pattern.compile("^@([a-zA-Z\\_]+[a-zA-Z0-9\\_\\.\\-]*)\\s*=\\s*\"(.*)\"\\s*$");
  private Pattern contPat=Pattern.compile( "^contains\\s*\\(\\s*text\\s*\\(\\s*\\)\\s*\\,\\s*\"(.*)\"\\s*\\)$");
  		
  public XPathEngineImpl() {
    // Do NOT add arguments to the constructor!!
  }
	
  // Set all the iXpath in the input implemantation
  public void setXPaths(String[] s) {
	for(String path: s) {
		this.paths.add(path.trim());
	}
  }

  // Get the root name of a XPath (without the beginning "/")
  private String getNodename(String step) {
	  Matcher match=this.namepat.matcher(step);
	  if(match.lookingAt()) {
		  return match.group(1);
	  }else {
		  return null;
	  }
  }
  
  // Return the position of the first closing bracket inside of which we have a test
  // with a valid definition according to XPath
  private int getTest(String input) {
	  input=input.trim();
	  
	  int end=-1;
	  int openbracket=1;
	  if(input.length()==0 || input.startsWith("/")) {
		  return 0;
	  } else if(!input.startsWith("[")) {
		  return -1;
	  }
	  char curr;
	  for(int i=1;i<input.length();i++) {
		  curr=input.charAt(i);
		  if(curr=='[') {
			  openbracket++;
		  }else if(curr==']') {
			  openbracket--;
			  if(openbracket==0) {
				  end=i;
				  break;
			  }
		  }
	  }
	  return end;
	  
  }
  
  // Check if an XPath test has a valid form
  private boolean isValidTest(String test) {
	  test=test.trim();
	  String reg="(^text\\s*\\(\\s*\\)\\s*=\\s*\".*\"\\s*$)|"+
	     "(^@([a-zA-Z\\_]+[a-zA-Z0-9\\_\\.\\-]*)\\s*=\\s*\".*\"\\s*$)|"+
		 "(^contains\\s*\\(\\s*text\\s*\\(\\s*\\)\\s*\\,\\s*\".*\"\\s*\\)$)";
	  Matcher match=Pattern.compile(reg).matcher(test);
	  if( match.matches()) {
		  return true;
	  }
	  return this.isValidStep(test);
  }
  
  // Check if an XPath is valid (without the first / in the beginning )
  private boolean isValidStep(String step) {
	  step=step.trim();
	  String Nodename=this.getNodename(step);
	  if(Nodename==null) {
		  return false;
	  }else {
		  String remName=step.substring(Nodename.length()).trim();
		  while(remName.length()>0) {
			  int end=this.getTest(remName);
			  if(end<0) {
				  return false;
			  }else if(end==0) {
				  break;
			  }
			  String test=remName.substring(1, end).trim();
		      if(!this.isValidTest(test)) {
		    	  return false;
		      }
		      remName=remName.substring(end+1).trim();
		  }
		  if(remName.length()==0) {
			  return true;
		  }
		  return this.isValidStep(remName.substring(1));
	  }
  }
  
  // Returns if the ith XPath (starting from 0) has a valid form
  public boolean isValid(int i) {
	try {
	String path=this.paths.get(i).trim();
	if(path.startsWith("/")) {
		return this.isValidStep(path.substring(1));
	}else {
		return false;
	}
	}catch(IndexOutOfBoundsException e) {
		return false;
	}
  }
  
  // Given a node performs, if the given test matches the node
  private boolean runTest(Node d,String test) {
	  try {
	  test=test.trim();
	  Matcher match=this.textPat.matcher(test);
	  if(match.matches()) {
		  // test case: text()="..."
		  String textContent=d.getTextContent();
		  return textContent!=null && textContent.contentEquals(match.group(1));
	  }
	  match=this.contPat.matcher(test);
	  if(match.matches()) {
		  // test case: contains(text(),"...")
		  String textContent=d.getTextContent();
		  return textContent!=null && textContent.contains(match.group(1));
	  }
	  match=this.attPat.matcher(test);
	  if(match.matches() && d.hasAttributes() ) {
		  // test case: @attname="..."
		  NamedNodeMap attMap=d.getAttributes();
		  Node attNode=attMap.getNamedItem(match.group(1).toLowerCase());
		  String value;
		  return attNode!=null && (value=attNode.getNodeValue())!=null && value.contentEquals(match.group(2));
		  
	  }
	  }catch(NullPointerException e) {
		  return false;
	  }
	  return this.searchMatchChild(d, "/"+test);
  }
  
  // Given a node, evaluate if a XPath matches the node
  private boolean nodeEvaluate(Node d,String path) {
	  path=path.trim();
	  String remName=path;
	  if(d.getNodeType()==Node.ENTITY_REFERENCE_NODE){
		  return this.searchMatchChild(d, path);
	  }
	  String Nodename=this.getNodename(path);
	  if(Nodename==null || !d.getNodeName().toLowerCase().contentEquals(Nodename.toLowerCase())) {
		  return false;
	  }
	  remName=path.substring(Nodename.length()).trim();
	  while(remName.length()>0) {
		  int end=this.getTest(remName);
		  if(end<0) {
			  return false;
		  }else if(end==0) {
			  break;
		  }
		  String test=remName.substring(1, end).trim();
	      if(!this.runTest(d,test)) {
	    	  return false;
	      }
	      remName=remName.substring(end+1).trim();
	  }
	  if(remName.length()==0) {
		  return true;
	  }
	  return this.searchMatchChild(d, remName);
  }
  
  // Search for all children of a node and evaluate the XPath in each one of them until you find a match
  private boolean searchMatchChild(Node d, String path) {
	  if(!path.startsWith("/"))
		  return false;
	  path=path.substring(1);
	  NodeList children=d.getChildNodes();
	  if(children==null) 
		  return false;
	  for(int i=0; i<children.getLength();i++) {
		  if(this.nodeEvaluate(children.item(i), path))
			  return true;
	  }
	  return false;
  }
  
  // Evaluate a document in all the XPath and return a boolean array with each evaluation result
  // The ith element of the result indicates whether the document matches the ith XPath
  public boolean[] evaluate(Document d) { 
	int num_paths=this.paths.size();
	if(num_paths==0) {
		return null;
	}
	boolean[] result=new boolean[num_paths];
	for(int i=0;i<num_paths;i++) {
		if(this.isValid(i)) {
			
			result[i]=this.searchMatchChild(d,this.paths.get(i));
		} else {
			result[i]=false;
		}
	}
    return result;
  }

@Override
public boolean isSAX() {
	// TODO Auto-generated method stub
	return false;
}

@Override
public boolean[] evaluateSAX(InputStream document, DefaultHandler handler) {
	// TODO Auto-generated method stub
	return null;
}
        
}
