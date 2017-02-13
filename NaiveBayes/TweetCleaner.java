/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package naiveclassifier;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author prakash
 */
//This class is used to pre process training data.
public class TweetCleaner {
//
//    public static void main(String[] args) throws FileNotFoundException, IOException {
//        BufferedReader br = new BufferedReader(new FileReader(new File("test.csv")));
//        String s = br.readLine();
//        String[] splitted;
//        String tweet;
//        while (s != null) {
//            splitted = s.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
//            try {
//                tweet = removeSpecalChars(removeReference(removeUrl(splitted[5])));
//                System.out.println("" + tweet);
//            } catch (Exception e) {
//                tweet = splitted[5];
//                System.out.println("" + tweet);
//            }
//            s = br.readLine();
//        }
//
//    }
//

    private static final String REMOVE_URL = "((https?|ftp|gopher|telnet|file|Unsure|http):((//)|(\\\\))+[\\w\\d:#@%/;$~_\\+-=\\\\\\.&]*)";
    private static final String REMOVE_REFERENCE = "((@)[\\\\w\\\\d:#@%/$~\\\\+-=\\\\\\\\\\\\&]*)";
    private static final String REMOVE_SPECIAL_CHARACTERS = "[^a-zA-Z0-9 ]";
    private static final String[] STOP_WORDS = {"a", "about", "above", "above", "across", "after", "afterwards", "again", "against", "all", "almost", "alone", "along", "already", "also", "although", "always", "am", "among",
        "amongst", "amoungst", "amount", "an", "and", "another", "any", "anyhow", "anyone", "anything",
        "anyway", "anywhere", "are", "around", "as", "at", "back", "be", "became", "because", "become",
        "becomes", "becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides",
        "between", "beyond", "bill", "both", "bottom", "but", "by", "call", "can", "cannot", "cant", "co",
        "con", "could", "couldnt", "cry", "de", "describe", "detail", "do", "done", "down", "due", "during",
        "each", "eg", "eight", "either", "eleven", "else", "elsewhere", "empty", "enough", "etc", "even",
        "ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fify", "fill",
        "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from", "front",
        "full", "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here",
        "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how",
        "however", "hundred", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its",
        "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me",
        "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must",
        "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody",
        "none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one",
        "only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own",
        "part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem", "seemed", "seeming",
        "seems", "serious", "several", "she", "should", "show", "side", "since", "sincere", "six", "sixty",
        "so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still", "such",
        "system", "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence",
        "there", "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thickv",
        "thin", "third", "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to",
        "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", "until", "up",
        "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence",
        "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether",
        "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with",
        "within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves"};

    // This function cleans tweets. Removes URL,References and special characters.
    public static String cleanTweet(String uncleanedTweet) {
        String cleanTweet = "";
        try {
            cleanTweet = removeSpecalChars(removeReference(removeUrl(uncleanedTweet)));
        } catch (Exception e) {
            cleanTweet = uncleanedTweet;
        }
        return cleanTweet;

    }

    private static String removeUrl(String commentstr) {
        String urlPattern = REMOVE_URL;
        Pattern p = Pattern.compile(urlPattern, Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(commentstr);
        while (m.find()) {
            commentstr = commentstr.replaceAll(m.group(), "").trim();
        }
        return commentstr;
    }

    private static String removeReference(String commentstr) {
        String urlPattern = REMOVE_REFERENCE;
        Pattern p = Pattern.compile(urlPattern, Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(commentstr);
        while (m.find()) {
            commentstr = commentstr.replaceAll(m.group(), "").trim();
        }
        return commentstr;
    }

    private static String removeSpecalChars(String commentstr) {
        Pattern pt = Pattern.compile(REMOVE_SPECIAL_CHARACTERS);
        Matcher match = pt.matcher(commentstr);
        while (match.find()) {
            String s = match.group();
            commentstr = commentstr.replaceAll("\\" + s, "");
        }
        return commentstr;
    }

    

}
