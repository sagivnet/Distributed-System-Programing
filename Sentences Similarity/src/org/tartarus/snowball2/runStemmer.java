package org.tartarus.snowball2;

import org.tartarus.snowball2.ext.englishStemmer;

/**
 * Gets an array of sentences to be lemmatized. Returns the lemmatized array of
 * sentences.
 * 
 * @author imyag
 *
 */
public class runStemmer {
	public static String englishStem(String sentence) {
		englishStemmer es = new englishStemmer();
		String[] splitted = sentence.split(" ");
		String curr = "";
		for (String word : splitted) {
			es.setCurrent(word);
			es.stem();
			curr += " " + es.getCurrent();
		}
		return new String(curr.trim());
	}
}
