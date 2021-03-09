package database;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class QueryParser {

	public void parsingQuery(String query) {

		if (query != null && query.trim().length() > 0) {

			query = query.trim();

			query.replaceAll("\\s+", " ");

			Pattern pattern = Pattern.compile("^(SELECT |UPDATE |INSERT |DELETE |CREATE TABLE |CREATE DATABASE )",
					Pattern.CASE_INSENSITIVE);
			Matcher matcher = pattern.matcher(query);
			boolean matchFound = matcher.find();
			if (matchFound) {
				String queryType = matcher.group();
				System.out.println("Match found");

				if (queryType.trim().equalsIgnoreCase("SELECT")) {

					tokenizeSelectQuery(pattern, matcher, query);

				} else if (queryType.trim().equalsIgnoreCase("UPDATE")) {

					tokenizeUpdateQuery(pattern, matcher, query);

				} else if (queryType.trim().equalsIgnoreCase("INSERT")) {

					tokenizeInsertQuery(pattern, matcher, query);

				} else if (queryType.trim().equalsIgnoreCase("DELETE")) {

					tokenizeDeleteQuery(pattern, matcher, query);

				} else if (queryType.trim().equalsIgnoreCase("CREATE TABLE")) {

					tokenizeCreateTableQuery(pattern, matcher, queryType);

				} else if (queryType.trim().equalsIgnoreCase("CREATE DATABASE")) {

					tokenizeCreateDatabaseQuery(pattern, matcher, queryType);

				}
			} else {
				System.out.println("Query syntax is not correct, please check keywords spellings and order.");
			}
		}
	}

	private void tokenizeSelectQuery(Pattern pattern, Matcher matcher, String query) {
		pattern = Pattern.compile("(SELECT)\\s+(\\*|[\\w, ]+)\\s+FROM\\s+([\\w]+)\\s*( WHERE\\s+(\\S)+)?$",
				Pattern.CASE_INSENSITIVE);
		matcher = pattern.matcher(query);

		if (matcher.find()) {
			String operation = matcher.group(1);
			String[] columns = matcher.group(2).split(",");
			String tableName = matcher.group(3);
			String conditions = matcher.group(5);
		} else {
			System.out.println("Query syntax is not correct, please check keywords spellings and order.");
		}
	}

	private void tokenizeUpdateQuery(Pattern pattern, Matcher matcher, String query) {
		pattern = Pattern.compile("(UPDATE)\\s+([\\w]+)\\s+SET\\s+(.+)\\s*( WHERE\\s+(\\S)+)?$",
				Pattern.CASE_INSENSITIVE);
		matcher = pattern.matcher(query);

		if (matcher.find()) {
			String operation = matcher.group(1);
			String tableName = matcher.group(2);
			String[] updateOperations = matcher.group(2).split(",");
			String conditions = matcher.group(5);
		} else {
			System.out.println("Query syntax is not correct, please check keywords spellings and order.");
		}
	}

	private void tokenizeInsertQuery(Pattern pattern, Matcher matcher, String query) {
		pattern = Pattern.compile(
				"(INSERT)\\s+INTO\\s+([\\w]+)\\s*(\\(([\\w, ]+)\\))?\\s*\\s+VALUES\\s+\\(([\\w, ]+)\\)$",
				Pattern.CASE_INSENSITIVE);
		matcher = pattern.matcher(query);

		if (matcher.find()) {
			String operation = matcher.group(1);
			String tableName = matcher.group(2);
			String[] columnNames = matcher.group(4).split(",");
			String[] columnValues = matcher.group(5).split(",");
		} else {
			System.out.println("Query syntax is not correct, please check keywords spellings and order.");
		}
	}

	private void tokenizeDeleteQuery(Pattern pattern, Matcher matcher, String query) {
		pattern = Pattern.compile("(DELETE)\\s+FROM\\s+([\\w]+)\\s*( WHERE\\s+(\\S)+)?$", Pattern.CASE_INSENSITIVE);
		matcher = pattern.matcher(query);

		if (matcher.find()) {
			String operation = matcher.group(1);
			String tableName = matcher.group(2);
			String conditions = matcher.group(4);
		} else {
			System.out.println("Query syntax is not correct, please check keywords spellings and order.");
		}
	}

	private void tokenizeCreateTableQuery(Pattern pattern, Matcher matcher, String query) {
		pattern = Pattern.compile("(CREATE)\\s+(TABLE)\\s+([\\w]+)\\s*\\(([\\w, \\(\\)]+)\\)$",
				Pattern.CASE_INSENSITIVE);
		matcher = pattern.matcher(query);

		if (matcher.find()) {
			String operation = matcher.group(1);
			String subOperation = matcher.group(2);
			String tableName = matcher.group(3);
			String columnsDesc = matcher.group(4);

			String[] columnDescArray = columnsDesc.split(",");

			JSONObject tableColumnsObject = new JSONObject();
			JSONObject tableForeignKeysObject = new JSONObject();
			String primaryKey = null;

			JSONArray foreignKeyArray = new JSONArray();
			JSONArray columnArray = new JSONArray();
			JSONObject foreignKeyObj = null;
			JSONObject columnObj = null;

			for (String columnDesc : columnDescArray) {
				pattern = Pattern.compile(
						"([\\w]+)\\s*( INT| VARCHAR)\\s*( PRIMARY KEY| REFERENCES\\s+([\\w]+)\\((\\w+)\\))\\s*?",
						Pattern.CASE_INSENSITIVE);
				matcher = pattern.matcher(columnDesc);
				if (matcher.find()) {
					columnObj = new JSONObject();
					String columnName = matcher.group(1);
					String columnType = matcher.group(2);
					String constraint = matcher.group(3);
					String foreignKeyTable = matcher.group(4);
					String foreignKeyColumn = matcher.group(5);

					columnObj.put("columnName", columnName.trim());
					columnObj.put("columnType", columnType.trim());
					columnArray.add(columnObj);

					if (constraint != null && constraint.trim().length() > 0
							&& constraint.trim().equalsIgnoreCase("PRIMARY KEY")) {
						primaryKey = columnName.trim();
					}

					if (foreignKeyTable != null && foreignKeyTable.trim().length() > 0) {
						foreignKeyObj = new JSONObject();
						foreignKeyObj.put("column", columnName);
						foreignKeyObj.put("foreignKeyTable", foreignKeyTable.trim());
						foreignKeyObj.put("foreignKeyColumn", foreignKeyColumn.trim());
						foreignKeyArray.add(foreignKeyObj);
					}

				} else {
					System.out.println("Please check syntax of the command.");
				}
				
				tableColumnsObject.put("columns", columnArray);
				tableForeignKeysObject.put("keys", foreignKeyArray);
			}
			
		} else {
			System.out.println("Query syntax is not correct, please check keywords spellings and order.");
		}
	}

	private void tokenizeCreateDatabaseQuery(Pattern pattern, Matcher matcher, String query) {
		pattern = Pattern.compile("(CREATE)\\s+(DATABASE)\\s+([\\w]+)$", Pattern.CASE_INSENSITIVE);
		matcher = pattern.matcher(query);

		if (matcher.find()) {
			String operation = matcher.group(1);
			String subOperation = matcher.group(2);
			String databaseName = matcher.group(3);
		} else {
			System.out.println("Query syntax is not correct, please check keywords spellings and order.");
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
}
