package database;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryParser {

	public static void parsingQuery(String query) {
		Pattern pattern = Pattern.compile("^(SELECT |UPDATE |INSERT |DELETE |CREATE )", Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(query);
		boolean matchFound = matcher.find();
		if (matchFound) {
			String queryType = matcher.group();
			System.out.println("Match found");

			if (queryType.trim().equalsIgnoreCase("SELECT")) {
				pattern = Pattern.compile("(SELECT)\\s+(\\*|[\\w, ]+)\\s+FROM\\s+([\\w]+)\\s*( WHERE\\s+(\\S)+)?$",
						Pattern.CASE_INSENSITIVE);
				matcher = pattern.matcher(query);
				if (matcher.find()) {
					System.out.println(matcher.groupCount());
					System.out.println("1 " + matcher.group(1));
					System.out.println("2 " + matcher.group(2));
					System.out.println("2 " + matcher.group(3));
					System.out.println("2 " + matcher.group(4));
					queryType = matcher.group();
					System.out.println("Match found");
				} else {
					System.out.println("NF");
				}

			} else if (queryType.trim().equalsIgnoreCase("UPDATE")) {
				pattern = Pattern.compile("(UPDATE)\\s+([\\w]+)\\s+SET\\s+(.+)\\s*( WHERE\\s+(\\S)+)?$",
						Pattern.CASE_INSENSITIVE);
				matcher = pattern.matcher(query);
				if (matcher.find()) {
					System.out.println(matcher.groupCount());
					System.out.println("1 " + matcher.group(1));
					System.out.println("2 " + matcher.group(2));
					System.out.println("2 " + matcher.group(3));
					System.out.println("2 " + matcher.group(4));
					queryType = matcher.group();
					System.out.println("Match found");
				} else {
					System.out.println("NF");
				}
			} else if (queryType.trim().equalsIgnoreCase("INSERT")) {
				pattern = Pattern.compile("(INSERT)\\s+INTO\\s+\\(([\\w, ]+)\\)\\s*([\\w, ]+)?\\s+VALUES\\s+\\(([\\w]+, )\\)$",
						Pattern.CASE_INSENSITIVE);
				matcher = pattern.matcher(query);
				if (matcher.find()) {
					System.out.println(matcher.groupCount());
					System.out.println("1 " + matcher.group(1));
					System.out.println("2 " + matcher.group(2));
					System.out.println("2 " + matcher.group(3));
					System.out.println("2 " + matcher.group(4));
					queryType = matcher.group();
					System.out.println("Match found");
				} else {
					System.out.println("NF");
				}
			} else if (queryType.trim().equalsIgnoreCase("DELETE")) {
				
				pattern = Pattern.compile("DELETE\\s+FROM\\s+([\\w]+)\\s*$",
						Pattern.CASE_INSENSITIVE);
				matcher = pattern.matcher(query);
				if (matcher.find()) {
					System.out.println(matcher.groupCount());
					System.out.println("1 " + matcher.group(1));
					System.out.println("2 " + matcher.group(2));
					System.out.println("2 " + matcher.group(3));
					System.out.println("2 " + matcher.group(4));
					queryType = matcher.group();
					System.out.println("Match found");
				} else {
					System.out.println("NF");
				}
			} else if (queryType.trim().equalsIgnoreCase("CREATE")) {
				pattern = Pattern.compile("CREATE\\s+TABLE\\s+([\\w]+)\\s*\\(([\\w, \\(\\)]+)($",
						Pattern.CASE_INSENSITIVE);
			}
		} else {
			System.out.println("Match not found");
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		System.out.println("Started 2");
		QueryParser.parsingQuery("select c1, c2 FROM table1 WHERE a");

	}
}
