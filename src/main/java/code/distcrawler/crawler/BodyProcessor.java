package code.distcrawler.crawler;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class BodyProcessor {
	
	private static String[] stopWordList = {"i", "me", "my", "myself", "we", "our", "ours", 
	                                        "ourselves", "you", "your", "yours", "yourself", "yourselves", 
	                                        "he", "him", "his", "himself", "she", "her", "hers", "herself", 
	                                        "it", "its", "itself", "they", "them", "their", "theirs", "themselves", 
	                                        "what", "which", "who", "whom", "this", "that", "these", "those", "am", 
	                                        "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", 
	                                        "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", 
	                                        "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", 
	                                        "with", "about", "against", "between", "into", "through", "during", "before", 
	                                        "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", 
	                                        "over", "under", "again", "further", "then", "once", "here", "there", "when", 
	                                        "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", 
	                                        "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", 
	                                        "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"};
	
	private static Set<String> stopWordSet = new HashSet<String>();
	
	public BodyProcessor() {
		for(String word: stopWordList) {
			stopWordSet.add(word);
		}
	}
	
	public static String process(String input) {
		if(input.length() == 0) {
			return "";
		}
		String[] rawBodyStringArray = input.split(" ");
		StringBuilder sb = new StringBuilder();
		for(String word: rawBodyStringArray) {
			String after = processWord(word);
			if(after.length() > 0) {
				sb.append(after + " ");
			}
		}
		if(sb.length() > 0) {
			sb.deleteCharAt(sb.length() - 1);			
		}
		
		return sb.toString();
	}
	
	private static String processWord(String word) {
		
		word = word.toLowerCase();
		
		// whether it exists in stopWordSet
		if(stopWordSet.contains(word)) { return ""; }
		
		// parse year
		if(word.length() == 4 && (word.charAt(0) == '1' || word.charAt(0) == '2')) { return word; }
		
		// parse the special character
		StringBuilder sb = new StringBuilder();
		int start = 0;
		while(start < word.length() && !Character.isLetter(word.charAt(start))) {
			start++;
		}
		
		while(start < word.length() && Character.isLetter(word.charAt(start))) {
			sb.append(word.charAt(start));
			start++;
		}
		
		word = sb.toString();
		
		return word;
	}
}
