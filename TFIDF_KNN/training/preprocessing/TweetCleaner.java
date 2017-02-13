/**
 * @author deva
 * 
 * This class is to clean the tweets - Remove Special characters, URLs and username tags
 * These elements of the tweet does not contribute towards the sentiments
 */
 
package preprocessing;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class TweetCleaner {

	public static String clean(String str){
		return removeSpecalChars(removeReference(removeUrl(str)));
	}

	
	// Removing URLs
    public static String removeUrl(String commentstr) {
        String urlPattern = "((https?|ftp|gopher|telnet|file|Unsure|http):((//)|(\\\\))+[\\w\\d:#@%/;$~_\\+-=\\\\\\.&]*)";
        Pattern p = Pattern.compile(urlPattern, Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(commentstr);
        while (m.find()) {
            commentstr = commentstr.replaceAll(m.group(), "").trim();
        }
        return commentstr;
    }
 // Removing username tags
    public static String removeReference(String commentstr) {
        String urlPattern = "((@)[\\w\\d:#@%/$~\\+-=\\\\\\&]*)";
        Pattern p = Pattern.compile(urlPattern, Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(commentstr);
        while (m.find()) {
            commentstr = commentstr.replaceAll(m.group(), "").trim();
        }
        return commentstr;
    }

    //Rempve special characters like "[^a-zA-Z0-9 ]"
    public static String removeSpecalChars(String commentstr) {
        Pattern pt = Pattern.compile("[^a-zA-Z0-9 ]");
        Matcher match = pt.matcher(commentstr);
        while (match.find()) {
            String s = match.group();
            commentstr = commentstr.replaceAll("\\" + s, "");
        }
        return commentstr;
    }

}
