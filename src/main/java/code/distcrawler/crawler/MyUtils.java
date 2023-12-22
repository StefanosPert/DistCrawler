package code.distcrawler.crawler;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import code.distcrawler.crawler.info.URLInfo;

public class MyUtils {
	public static Date dateparser(String input) {
		// Receives an input string and tries to parse it as a date.
		// If the string is a date that follows one of the 3 HTTP supported
		// formats, it returns a Date object with that date
		SimpleDateFormat[] format=new SimpleDateFormat[3];
		ParsePosition pos=new ParsePosition(0);
		Date result=null;
		// Removing all the white spaces in the beginning of the string 
		String regex="^\\s+";
		input=input.replaceAll(regex, "");
		
		format[0]=new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss z");
		format[1]=new SimpleDateFormat("EEE, d-MMM-yyyy HH:mm:ss z");
		format[2]=new SimpleDateFormat("EEE MMM d HH:mm:ss yyyy z");
		for(int i=0;(i<3 && pos.getIndex()==0);i++) {
				result=format[i].parse(input,pos);

		}
		return result;
	}
	
	public static String unencodeOctets(String input) {
		// Unencodes URL encoded octets in the input string
		try {
		String reg="(%[a-fA-f0-9]{2}?)";
		Pattern pat=Pattern.compile(reg);
		Matcher matching=pat.matcher(input);
		while(matching.lookingAt()) {
			String matchChar=matching.group(1);
			String decodedChar=URLDecoder.decode(matchChar,"UTF-8");
			if(!decodedChar.contentEquals("/")) {
				input=input.replace(matchChar, decodedChar);
			}
			matching=pat.matcher(input);
		}
		}catch(UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return input;
	}
	
	
	public static String getAbsoluteURL(String url, URLInfo currURL) {
		// Given a URL link and the URL of the page from which is extracted, return the absolute URL 
		if(url.startsWith("http://") || url.startsWith("https://")) {
			return url;
		}
		if(url.startsWith("//")) {
			return currURL.getProtocol().toLowerCase()+":"+url;
		}
		if(url.startsWith("/")) {
			return currURL.getProtocol().toLowerCase()+"://"+currURL.getHostName()+":"+currURL.getPortNo()+url;
		}
		int lastSlash=currURL.getFilePath().lastIndexOf('/');
		String prefix_path=currURL.getFilePath().substring(0, lastSlash+1);
		return currURL.getProtocol().toLowerCase()+"://"+currURL.getHostName()+":"+currURL.getPortNo()+prefix_path+url;
	}
	
	public static String getNormalizedUrl(String url) {
		if(url.length() < 11) { return ""; }
		StringBuilder sb = new StringBuilder(url);
		if(url.startsWith("http://") || url.startsWith("https://")) {
			int insertIdx = sb.indexOf("/", 10);

			if(url.startsWith("http://")) {
				if(url.indexOf(":80") != -1) { return url; }
				if(insertIdx == -1) { sb.append(":80"); }
				else { sb.insert(insertIdx, ":80"); }
			}
			
			if(url.startsWith("https://")) {
				if(url.indexOf(":443") != -1) { return url; }
				if(insertIdx == -1) { sb.append(":443"); }
				else { sb.insert(insertIdx, ":443"); }
			}
				
		} else {
			return "";
		}
		return sb.toString();
	}
	
}
