package com.badalb.spark;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class WordCountJavaOld {

	public static void main(String[] args) {

		String text = "the quick brown fox jumps over the lazy dog" 
		+ " the quick brown fox jumps over the lazy dog";
		StringTokenizer tokenizer = new StringTokenizer(text, " ");

		Map<String, Integer> wcount = new HashMap<String, Integer>();

		while (tokenizer.hasMoreTokens()) {
			String str = tokenizer.nextToken();

			if (wcount.containsKey(str)) {
				wcount.put(str, wcount.get(str) + 1);
			} else {
				wcount.put(str, 1);
			}
		}

		System.out.println(wcount);
	}

}
