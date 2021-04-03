package database;

import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import sun.tools.jconsole.Tab;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;


public class QueryParser {

    private static String databaseName;

    public QueryParser() {
    }

    public boolean parsingQuery(String query, boolean isTransaction) {

        if (query != null && query.trim().length() > 0) {

            query = query.trim();

            query.replaceAll("\\s+", " ");

            Pattern pattern = Pattern.compile(
                    "^(SELECT |UPDATE |INSERT |DELETE |CREATE TABLE |CREATE DATABASE |USE DATABASE |BEGIN TRANSACTION |DROP TABLE |TRUNCATE TABLE)",
                    Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(query);
            boolean matchFound = matcher.find();
            if (matchFound) {
                String queryType = matcher.group();
                System.out.println("Match found");

                if (queryType.trim().equalsIgnoreCase("CREATE DATABASE") && !isTransaction) {
                    tokenizeCreateDatabaseQuery(pattern, matcher, query);
                } else if (queryType.trim().equalsIgnoreCase("USE DATABASE") && !isTransaction) {
                    tokenizeUseDatabaseQuery(pattern, matcher, query);
                } else if (StringUtils.isBlank(QueryParser.databaseName)) {
                    System.out.println("Please specify database");
                } else if (queryType.trim().equalsIgnoreCase("SELECT") && !isTransaction) {
                    tokenizeSelectQuery(pattern, matcher, query);
                } else if (queryType.trim().equalsIgnoreCase("UPDATE")) {
                    tokenizeUpdateQuery(pattern, matcher, query, isTransaction);
                } else if (queryType.trim().equalsIgnoreCase("INSERT")) {
                    tokenizeInsertQuery(pattern, matcher, query, isTransaction);
                } else if (queryType.trim().equalsIgnoreCase("DELETE")) {
                    tokenizeDeleteQuery(pattern, matcher, query, isTransaction);
                } else if (queryType.trim().equalsIgnoreCase("CREATE TABLE") && !isTransaction) {
                    tokenizeCreateTableQuery(pattern, matcher, query);
                } else if (queryType.trim().equalsIgnoreCase("BEGIN TRANSACTION") && !isTransaction) {
                    tokenizeTransactionQuery(pattern, matcher, query);
                } else if (queryType.trim().equalsIgnoreCase("DROP TABLE") && !isTransaction) {
                    tokenizeDropQuery(pattern, matcher, query);
                } else if (queryType.trim().equalsIgnoreCase("TRUNCATE TABLE") && !isTransaction) {
                    tokenizeTruncateQuery(pattern, matcher, query);
                }
            } else {
                System.out.println("Query syntax is not correct, please check keywords spellings and order.");
                return false;
            }
        }
        return true;
    }

    private void tokenizeTransactionQuery(Pattern pattern, Matcher matcher, String query) {
        try {
            TransactionManager transactionManager = new TransactionManager(databaseName);
            BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
            String line = "";
            while (!line.equalsIgnoreCase("END")) {
                line = in.readLine();
                if (parsingQuery(line, true)) {
                    transactionManager.addTransaction(line);
                }
            }
            in.close();
            transactionManager.endAndExecuteTransaction();
        } catch (Exception e) {
            System.out.println("Transaction failed");
            e.printStackTrace();
        }
    }

    private void tokenizeInsertQuery(Pattern pattern, Matcher matcher, String query, boolean isTransaction) {
        pattern = Pattern.compile(
                "(INSERT)\\s+INTO\\s+([\\w]+)\\s*(\\(([\\w, ]+)\\))?\\s*\\s+VALUES\\s+\\(([\\w, ]+)\\)$",
                Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(query);

        if (matcher.find()) {
            String operation = matcher.group(1);
            String tableName = matcher.group(2);
            String columnName = matcher.group(4);
            String[] columnNames = null;
            if (columnName != null) {
                columnNames = columnName.split(",");
            }
            String[] columnValues = matcher.group(5).split(",");

            System.out.println(Arrays.asList(columnNames));
            System.out.println(Arrays.asList(columnValues));

            try {
                TableValidations tableValidations = new TableValidations("Schema", databaseName, columnNames, columnValues);
                if (tableValidations.checkPrimaryKey(tableName)
                        && tableValidations.checkForeignKey(tableName)
                        && tableValidations.checkDataTypes() && !isTransaction) {
                    InsertQueryExecutor insertQueryExecutor = new InsertQueryExecutor(tableName, databaseName);
                    insertQueryExecutor.performInsertQueryOperation(columnValues);
                } else {
                    System.out.println("Constraint error");
                }
            } catch (IOException e) {
                System.out.println("Cannot write to database" + e.getMessage());
                e.getStackTrace();
            }

        } else {
            System.out.println("Query syntax is not correct, please check keywords spellings and order.");
        }
    }

    private void tokenizeDeleteQuery(Pattern pattern, Matcher matcher, String query, boolean isTransaction) {
        pattern = Pattern.compile("(DELETE)\\s+FROM\\s+([\\w]+)\\s* WHERE\\s+(([\\S]+))?$", Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(query);

        if (matcher.find()) {
            String operation = matcher.group(1);
            String tableName = matcher.group(2);
            String conditions = matcher.group(3);
            if (!isTransaction) {
                DeleteQueryExecutor deleteQueryExecutor = new DeleteQueryExecutor(tableName, databaseName);
                deleteQueryExecutor.splitCondition(conditions);
            }
        } else {
            System.out.println("Query syntax is not correct, please check keywords spellings and order.");
        }
    }

    private void tokenizeSelectQuery(Pattern pattern, Matcher matcher, String query) {
        pattern = Pattern.compile("(SELECT)\\s+(\\*|[\\w, ]+)\\s+FROM\\s+([\\w]+)\\s*( WHERE\\s+([\\S]+))?$",
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

    private void tokenizeUpdateQuery(Pattern pattern, Matcher matcher, String query, boolean isTransaction) {
        pattern = Pattern.compile("(UPDATE)\\s+([\\w]+)\\s+SET\\s+([\\S]+)\\s*( WHERE\\s+([\\S]+))?$",
                Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(query);

        if (matcher.find()) {
            String operation = matcher.group(1);
            String tableName = matcher.group(2);
            String updateOperations = matcher.group(3);
            String conditions = matcher.group(5);
            if (!isTransaction) {
                UpdateQueryExecutor uqe = new UpdateQueryExecutor(tableName, databaseName);
                uqe.executeUpdateMain(updateOperations, conditions);
            }
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

            List<String> columns = new ArrayList<String>();
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
                    columns.add(columnName.trim());
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
            		columns, tableColumnsObject, tableForeignKeysObject);

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

    private void tokenizeDropQuery(Pattern pattern, Matcher matcher, String query) {
        pattern = Pattern.compile("(DROP)\\s+(TABLE)\\s+([\\w]+)\\s*$", Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(query);

        if (matcher.find()) {
            String tableName = matcher.group(3);
            try {
                DropQueryExecutor dropQueryExecutor = new DropQueryExecutor(databaseName);
                dropQueryExecutor.performDropQueryOperation(tableName);
            } catch (IOException e) {
                System.out.println("Cannot DROP table" + e.getMessage());
                e.getStackTrace();
            }
        } else {
            System.out.println("Query syntax is not correct, please check keywords spellings and order.");
        }
    }

    private void tokenizeTruncateQuery(Pattern pattern, Matcher matcher, String query) {
        pattern = Pattern.compile("(TRUNCATE)\\s+(TABLE)\\s+([\\w]+)\\s*$", Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(query);

        if (matcher.find()) {
            String tableName = matcher.group(3);
            try {
                TruncateQueryExecutor truncateQueryExecutor = new TruncateQueryExecutor(databaseName);
                truncateQueryExecutor.performTruncateQueryOperation(tableName);
            } catch (IOException e) {
                System.out.println("Cannot TRUNCATE table" + e.getMessage());
                e.getStackTrace();
            }
        } else {
            System.out.println("Query syntax is not correct, please check keywords spellings and order.");
        }
    }

    public static void main(String[] args) {
        QueryParser parser = new QueryParser();
        String query = "USE DATABASE DemoDB";
        parser.parsingQuery(query, false);
        query = "INSERT INTO Demo(Id,Name,Age,Married,DepartmentId,VehicleId) VALUES (1,jay,20,false,4,4)";
        //query = "DELETE FROM Demo WHERE Id!=1";
        //query = "TRUNCATE TABLE Demo";
        //query = "DROP TABLE Demo";
        parser.parsingQuery(query, false);
    }
}
