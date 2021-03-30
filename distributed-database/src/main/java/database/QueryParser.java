package database;

import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryParser {

    private static String databaseName;

    public void parsingQuery(String query) {

        if (query != null && query.trim().length() > 0) {

            query = query.trim();

            query.replaceAll("\\s+", " ");

            Pattern pattern = Pattern.compile(
                    "^(SELECT |UPDATE |INSERT |DELETE |CREATE TABLE |CREATE DATABASE |USE DATABASE )",
                    Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(query);
            boolean matchFound = matcher.find();
            if (matchFound) {
                String queryType = matcher.group();
                System.out.println("Match found");

                if (queryType.trim().equalsIgnoreCase("CREATE DATABASE")) {
                    tokenizeCreateDatabaseQuery(pattern, matcher, query);
                } else if (queryType.trim().equalsIgnoreCase("USE DATABASE")) {
                    tokenizeUseDatabaseQuery(pattern, matcher, query);
                } else if (StringUtils.isBlank(QueryParser.databaseName)) {
                    System.out.println("Please specify database");
                } else if (queryType.trim().equalsIgnoreCase("SELECT")) {
                    tokenizeSelectQuery(pattern, matcher, query);
                } else if (queryType.trim().equalsIgnoreCase("UPDATE")) {
                    tokenizeUpdateQuery(pattern, matcher, query);
                } else if (queryType.trim().equalsIgnoreCase("INSERT")) {
                    tokenizeInsertQuery(pattern, matcher, query);
                } else if (queryType.trim().equalsIgnoreCase("DELETE")) {
                    tokenizeDeleteQuery(pattern, matcher, query);
                } else if (queryType.trim().equalsIgnoreCase("CREATE TABLE")) {
                    tokenizeCreateTableQuery(pattern, matcher, query);
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
            String columns = matcher.group(2);
            String tableName = matcher.group(3);
            String conditions = matcher.group(5);
            SelectQueryExecutor sqe = new SelectQueryExecutor(tableName, databaseName);
            sqe.executeSelectMain(operation, columns, conditions);
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
            String updateOperations = matcher.group(2);
            String conditions = matcher.group(5);
            UpdateQueryExecutor uqe = new UpdateQueryExecutor(tableName, databaseName);
            uqe.executeUpdateMain(updateOperations, conditions);
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
                        "([\\w]+)\\s*( INT| VARCHAR)\\s*( PRIMARY KEY| REFERENCES\\s+([\\w]+)\\((\\w+)\\))?\\s*$",
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
            }
            tableColumnsObject.put("columns", columnArray);
            tableForeignKeysObject.put("keys", foreignKeyArray);
            CreateTableQuery createTableQuery = new CreateTableQuery();
            createTableQuery.exceuteCreateTableQuery(QueryParser.databaseName, tableName, primaryKey,
                    tableColumnsObject, tableForeignKeysObject);

        } else {
            System.out.println("Query syntax is not correct, please check keywords spellings and order.");
        }
    }

    private void tokenizeCreateDatabaseQuery(Pattern pattern, Matcher matcher, String query) {
        pattern = Pattern.compile("(CREATE)\\s+(DATABASE)\\s+([\\w]+)\\s*$", Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(query);

        if (matcher.find()) {
            String databaseName = matcher.group(3);
            CreateDatabaseQuery createDatabaseQuery = new CreateDatabaseQuery();
            createDatabaseQuery.exceuteCreateDatabaseQuery(databaseName);
        } else {
            System.out.println("Query syntax is not correct, please check keywords spellings and order.");
        }
    }

    private void tokenizeUseDatabaseQuery(Pattern pattern, Matcher matcher, String query) {
        pattern = Pattern.compile("(USE)\\s+(DATABASE)\\s+([\\w]+)\\s*$", Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(query);

        if (matcher.find()) {
            String databaseName = matcher.group(3);
            QueryParser.databaseName = databaseName;
        } else {
            System.out.println("Query syntax is not correct, please check keywords spellings and order.");
        }
    }

    public static void main(String[] args) {
        QueryParser parser = new QueryParser();
        String query = "CREATE DATABASE test";
        parser.parsingQuery(query);
        query = "USE DATABASE test";
        parser.parsingQuery(query);
        query = "CREATE TABLE Persons_2 (PersonID int PRIMARY KEY,LastName varchar, FirstName varchar REFERENCES Persons(PersonID), Address varchar, City varchar)";
        parser.parsingQuery(query);

    }
}
