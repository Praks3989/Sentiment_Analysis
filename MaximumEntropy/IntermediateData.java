/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package wordcount;

/**
 *
 * @author deva
 */
public class IntermediateData {

    String tweetId, tweetClass, word;

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }
    double f;//fi
    double l;//lambda

    public String getTweetId() {
        return tweetId;
    }

    public void setTweetId(String tweetId) {
        this.tweetId = tweetId;
    }

    public String getTweetClass() {
        return tweetClass;
    }

    public void setTweetClass(String tweetClass) {
        this.tweetClass = tweetClass;
    }

    public double getF() {
        return f;
    }

    public void setF(double f) {
        this.f = f;
    }

    public double getL() {
        return l;
    }

    public void setL(double l) {
        this.l = l;
    }

}
