/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package wordcount;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author deva
 */
public class TweetCleaner {

    public static String cleanTweet(String data) {
        return removeSpecalChars(removeReference(removeUrl(data)));
    }

    private static String removeUrl(String commentstr) {
        String urlPattern = "((https?|ftp|gopher|telnet|file|Unsure|http):((//)|(\\\\))+[\\w\\d:#@%/;$~_\\+-=\\\\\\.&]*)";
        Pattern p = Pattern.compile(urlPattern, Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(commentstr);
        while (m.find()) {
            commentstr = commentstr.replaceAll(m.group(), "").trim();
        }
        return commentstr;
    }

    private static String removeReference(String commentstr) {
        String urlPattern = "((@)[\\w\\d:#@%/$~\\+-=\\\\\\&]*)";
        Pattern p = Pattern.compile(urlPattern, Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(commentstr);
        while (m.find()) {
            commentstr = commentstr.replaceAll(m.group(), "").trim();
        }
        return commentstr;
    }

    //"[^a-zA-Z0-9 ]"
    private static String removeSpecalChars(String commentstr) {
        Pattern pt = Pattern.compile("[^a-zA-Z0-9 ]");
        Matcher match = pt.matcher(commentstr);
        while (match.find()) {
            String s = match.group();
            commentstr = commentstr.replaceAll("\\" + s, "");
        }
        return commentstr;
    }

}
