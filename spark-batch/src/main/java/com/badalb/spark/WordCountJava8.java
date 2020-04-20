package com.badalb.spark;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class WordCountJava8 {

	public static void main(String[] args) {

		String text = "the quick brown fox jumps over the lazy dog the quick brown fox jumps over the lazy dog";

		Map<String, Long> result = Arrays.asList(text.split(" ")).stream()
				.collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
		System.out.println(result);
	}
}
