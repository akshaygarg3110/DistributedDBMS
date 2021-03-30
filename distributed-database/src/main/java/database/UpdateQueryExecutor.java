package database;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


public class UpdateQueryExecutor {

    private String tableName;
    private String databaseName;
    private Map<Integer, String> fieldMap;

    public UpdateQueryExecutor(String tableName, String databaseName) {
        this.tableName = tableName;
        this.databaseName = databaseName;
    }

    BufferedReader getTableReader() throws Exception {
        String tablePath = this.databaseName + '/' + this.tableName + ".txt";
        return new BufferedReader(new FileReader(tablePath));
    }

    void populateColumnMap() {
        try {
            BufferedReader tableReader = getTableReader();
            String rows;
            rows = tableReader.readLine();
            String[] columns = rows.split("\\$");
            fieldMap = new HashMap<>();
            for (int i = 0; i < columns.length; i++) {
                fieldMap.put(i, columns[i]);
            }
            tableReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    int getIndexOfKeyName(String keyName) {
        populateColumnMap();
        for (Entry<Integer, String> columns : fieldMap.entrySet()) {
            if (columns.getValue().equalsIgnoreCase(keyName)) {
                return (int) columns.getKey();
            }
        }
        return -1;
    }

    public void performUpdateOnARow(String keyName, String keyValue, String updateKey, String updateValue, int compOperation) {
        SelectQueryExecutor selectQueryExecutor = new SelectQueryExecutor(tableName, databaseName);
        Map<Integer, List<String>> resultSet = selectQueryExecutor.executeSelectStatementWithFullColumns();
        int indexOfKey = getIndexOfKeyName(keyName);
        int indexOfUpdatedColumn = getIndexOfKeyName(updateKey);
        if (indexOfKey == -1 || indexOfUpdatedColumn == -1) {
            throw new IllegalArgumentException("Invalid key constraint");
        }
        for (Entry<Integer, List<String>> rows : resultSet.entrySet()) {
            String[] rowVal;
            rowVal = new String[rows.getValue().size()];
            rows.getValue().toArray(rowVal);
            String presentValue = rows.getValue().get(indexOfKey);
            if (constraintCheck(presentValue, keyValue, compOperation)) {
                rowVal[indexOfUpdatedColumn] = updateValue;
            }
            resultSet.put(rows.getKey(), Arrays.asList(rowVal));
        }
        insertRowDetailsInFile(resultSet);
    }

    private boolean constraintCheck(String presentValue, String keyValue, int compOperation) {
        switch (compOperation) {
            case 0:
                return presentValue.equalsIgnoreCase(keyValue);
            case 1:
                return Integer.parseInt(presentValue) > Integer.parseInt(keyValue);
            case 2:
                return Integer.parseInt(presentValue) < Integer.parseInt(keyValue);
            case 3:
                return !presentValue.equalsIgnoreCase(keyValue);
            case 4:
                return Integer.parseInt(presentValue) >= Integer.parseInt(keyValue);
            case 5:
                return Integer.parseInt(presentValue) <= Integer.parseInt(keyValue);
            default:
                return false;

        }
    }

    private boolean insertRowDetailsInFile(Map<Integer, List<String>> resultSet) {
        try {
            try (FileWriter insertRecord = new FileWriter(databaseName + "/" + tableName + ".txt")) {
                String headerLine = String.join("$", fieldMap.values());
                System.out.println(headerLine);
                insertRecord.write(headerLine);
                insertRecord.write("\n");
                for (Entry<Integer, List<String>> rows : resultSet.entrySet()) {
                    List<String> rowData = rows.getValue();
                    String line = String.join("$", rowData);
                    insertRecord.write(line);
                    insertRecord.write("\n");
                }
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    void executeUpdateMain(String operations, String condition) {
        if (operations != null && condition != null) {
            String[] operationPairs = operations.split(",");
            for (String operation : operationPairs) {
                String[] operands = operation.split("=");
                int comparisonOperation = fetchOperationType(condition);
                String operator = fetchOperation(condition);
                String[] conditionParameters = condition.split(operator);
                System.out.println(conditionParameters[0]);
                performUpdateOnARow(conditionParameters[0], conditionParameters[1],
                        operands[0], operands[1], comparisonOperation);
            }
            System.out.println("Updated successfully in the table!");
        }

    }

    private int fetchOperationType(String condition) {
        if (condition.contains("!=")) {
            return 3;
        }
        if (condition.contains(">=")) {
            return 4;
        }
        if (condition.contains("<=")) {
            return 5;
        }
        if (condition.contains("=")) {
            return 0;
        }
        if (condition.contains(">")) {
            return 1;
        }
        if (condition.contains("<")) {
            return 2;
        }

        return -1;
    }

    private String fetchOperation(String condition) {
        if (condition.contains(">=")) {
            return ">=";
        }
        if (condition.contains("!=")) {
            return "!=";
        }
        if (condition.contains("<=")) {
            return "<=";
        }
        if (condition.contains("=")) {
            return "=";
        }
        if (condition.contains(">")) {
            return ">";
        }
        if (condition.contains("<")) {
            return "<";
        }
        return "";
    }

    public static void main(String[] args) {
        UpdateQueryExecutor uqe = new UpdateQueryExecutor("students", "test");
        String operations = "csci5408=30";
        String conditions = "English>=90";
        uqe.executeUpdateMain(operations, conditions);
    }

}
