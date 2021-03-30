package database;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
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
        String tablePath = this.databaseName + '/' + this.tableName;
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
            if (columns.getValue().toString().equals(keyName)) {
                return (int) columns.getKey();
            }
        }
        return -1;
    }

    public boolean performUpdateOnARow(String keyName, String keyValue, String updateKey, String updateValue) {
        SelectQueryExecutor selectQueryExecutor = new SelectQueryExecutor(tableName, databaseName);
        Map<Integer, List<String>> resultSet = selectQueryExecutor.executeSelectStatementWithFullColumns();
        int indexOfKey = getIndexOfKeyName(keyName);
        int indexOfUpdatedColumn = getIndexOfKeyName(updateKey);
        if (indexOfKey == -1) {
            throw new IllegalArgumentException("Invalid key constraint");
        }
        for (Entry<Integer, List<String>> rows : resultSet.entrySet()) {
            List<String> rowDetails = rows.getValue();
            String presentValue = rowDetails.get(indexOfKey);
            if (presentValue.equalsIgnoreCase(updateValue)) {
                rowDetails.add(indexOfUpdatedColumn, updateValue);
            }
            resultSet.put(rows.getKey(), rowDetails);
        }
        return insertRowDetailsInFile(resultSet);
    }

    private boolean insertRowDetailsInFile(Map<Integer, List<String>> resultSet) {
        try {
            try (FileWriter insertRecord = new FileWriter(databaseName + "/" + tableName)) {
                String headerLine = String.join("\\$", fieldMap.values());
                insertRecord.write(headerLine);
                insertRecord.write("\n");
                for (Entry<Integer, List<String>> rows : resultSet.entrySet()) {
                    List<String> rowData = rows.getValue();
                    String line = String.join("\\$", rowData);
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

    void executeUpdateMain(String[] operations, String condition) {

    }

}
