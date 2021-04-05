package database;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


public class UpdateQueryExecutor {

    private String tableName;
    private String databaseName;
    private Map<Integer, String> fieldMap;
    private String location;
    private static final String DATABASE_ROOT_PATH = "Database";
    private static final Object REMOTE_URL = "https://storage.googleapis.com/5408_project_team6/Database";

    public UpdateQueryExecutor(String tableName, String databaseName, String location) {
        this.tableName = tableName;
        this.databaseName = databaseName;
        this.location = location;
    }

    BufferedReader getTableReader() throws Exception {
        String tablePath = "Database" + "/" + this.databaseName + '/' + this.tableName + ".txt";
        return new BufferedReader(new FileReader(tablePath));
    }

    void populateColumnMap() {
        try {
            BufferedReader tableReader;
            if (location.equalsIgnoreCase("REMOTE")) {
                URL url = new URL(REMOTE_URL + "/" + databaseName + "/" + tableName);
                tableReader = new BufferedReader(
                        new InputStreamReader(url.openStream()));
            } else {
                String tablePath = DATABASE_ROOT_PATH + "/" + this.databaseName + '/' + this.tableName + ".txt";
                tableReader = new BufferedReader(new FileReader(tablePath));
            }

            String rows;
            rows = tableReader.readLine();
            rows = tableReader.readLine();
            System.out.println(rows);
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
        System.out.println(fieldMap);
        for (Entry<Integer, String> columns : fieldMap.entrySet()) {
            if (columns.getValue().equalsIgnoreCase(keyName)) {
                return (int) columns.getKey();
            }
        }
        return -1;
    }

    public void performUpdateOnARow(String keyName, String keyValue, String updateKey, String updateValue, int compOperation) {
        SelectQueryExecutor selectQueryExecutor = new SelectQueryExecutor(tableName, databaseName, location);
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
        System.out.println(resultSet);
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
            try {
                FileWriter insertRecord = new FileWriter("Database/" + databaseName + "/" + tableName + ".txt");
                for (Entry<Integer, List<String>> rows : resultSet.entrySet()) {
                    List<String> rowData = rows.getValue();
                    String line = String.join("$", rowData);
                    insertRecord.write(line);
                    insertRecord.write("\n");
                }
                insertRecord.close();
                if (location.equalsIgnoreCase("REMOTE")) {
                    RemoteFileHandler rfh = new RemoteFileHandler(databaseName, tableName);
                    rfh.uploadObject();
                }
            } catch (Exception e) {
                System.out.println("unable to update");
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
        UpdateQueryExecutor uqe = new UpdateQueryExecutor("testStudents18", "dw", "remote");
        String operations = "age=80";
        String conditions = "student_id=2";
        uqe.executeUpdateMain(operations, conditions);
    }

}
