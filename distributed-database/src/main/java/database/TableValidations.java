package database;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableValidations {
    private String tableName;
    private String databaseName;
    private String[] columns;
    private String[] values;
    private Map<Integer, String> fieldMap;
    private Map<String, String> inputFieldMap = new HashMap<>();

    public TableValidations(String tableName, String databaseName, String[] columns, String[] values) {
        this.tableName = tableName;
        this.databaseName = databaseName;
        this.columns = columns;
        this.values = values;
        for (int i = 0; i < columns.length; i++) {
            inputFieldMap.put(columns[i], values[i]);
        }
    }

    BufferedReader getTableReader() throws Exception {
        String tablePath = this.databaseName + '/' + this.tableName;
        return new BufferedReader(new FileReader(tablePath));
    }

    public void populateColumnMap() {
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
        for (Map.Entry<Integer, String> columns : fieldMap.entrySet()) {
            if (columns.getValue().toString().equals(keyName)) {
                return (int) columns.getKey();
            }
        }
        return -1;
    }

    public void checkPrimaryKey(String actualTableName) {
        // selecting all records of Schema table
        SelectQueryExecutor selectQueryExecutor = new SelectQueryExecutor(tableName, databaseName);
        Map<Integer, List<String>> resultSet = selectQueryExecutor.executeSelectStatementWithFullColumns();

        // position of primary_key column in schema file
        int indexOfKey = getIndexOfKeyName("primary_key");
        // looping the schema table records
        for (Map.Entry<Integer, List<String>> rows : resultSet.entrySet()) {
            try {
                List<String> rowDetails = rows.getValue();
                if (Boolean.parseBoolean(rowDetails.get(indexOfKey))) {
                    int columnNameIndex = getIndexOfKeyName("column_name");
                    String columnName = rowDetails.get(columnNameIndex);
                    SelectQueryExecutor selectQueryExecutorActual = new SelectQueryExecutor(actualTableName, databaseName);
                    List<String> list = new ArrayList<String>();
                    list.add(columnName);
                    selectQueryExecutorActual.setFieldList(list);
                    Map<Integer, List<String>> resultSetActual = selectQueryExecutorActual.executeSelectStatementWithColumnList();
                    for (List<String> i : resultSetActual.values()) {
                        if (i.get(0).equals(inputFieldMap.get(columnName))) {
                            System.out.println("Primary Key Violation, value already exists!!!");
                        }
                    }
                }
            } catch (ArrayIndexOutOfBoundsException e) {

            }
        }
    }

    public void checkForeignKey(String actualTable) {
        SelectQueryExecutor selectQueryExecutor = new SelectQueryExecutor(tableName, databaseName);
        Map<Integer, List<String>> resultSet = selectQueryExecutor.executeSelectStatementWithFullColumns();

        int indexOfKey = getIndexOfKeyName("foreign_key");
        for (Map.Entry<Integer, List<String>> rows : resultSet.entrySet()) {
            try {
                List<String> rowDetails = rows.getValue();
                if (Boolean.parseBoolean(rowDetails.get(indexOfKey))) {
                    int columnNameIndex = getIndexOfKeyName("column_name");
                    int foreignKeyColumnNameIndex = getIndexOfKeyName("foreign_key_column_name");
                    int foreignKeyTableIndex = getIndexOfKeyName("foreign_key_table_name");

                    String columnName = rowDetails.get(columnNameIndex);
                    String foreignKeyColumnName = rowDetails.get(foreignKeyColumnNameIndex);
                    String foreignKeyTableName = rowDetails.get(foreignKeyTableIndex);

                    String foreignKeyValue = inputFieldMap.get(columnName);

                    List<String> list = new ArrayList<String>();
                    list.add(foreignKeyColumnName);
                    // TODO: foreignKeyTableName cannot have white spaces.
                    SelectQueryExecutor dependentSelectQueryExecutor = new SelectQueryExecutor(foreignKeyTableName + ".csv", databaseName);
                    dependentSelectQueryExecutor.setFieldList(list);
                    Map<Integer, List<String>> dependentResultSet = dependentSelectQueryExecutor.executeSelectStatementWithColumnList();

                    for (List<String> i : dependentResultSet.values()) {
                        if (i.get(0).equals(foreignKeyValue)) {
                            System.out.println("Foreign Key Exists, good to insert!!");
                        }
                    }
                }
            } catch (ArrayIndexOutOfBoundsException e) {
            }
        }
    }

    public void checkDataTypes() {
        SelectQueryExecutor selectQueryExecutor = new SelectQueryExecutor(tableName, databaseName);
        Map<Integer, List<String>> resultSet = selectQueryExecutor.executeSelectStatementWithFullColumns();
        if (validateColumnCount(columns, values)) {
            Map<String, String> dataTypeMap = new HashMap<>();
            for (Map.Entry<Integer, List<String>> rows : resultSet.entrySet()) {
                try {
                    List<String> rowDetails = rows.getValue();
                    int columnNameIndex = getIndexOfKeyName("column_name");
                    int dataTypeIndex = getIndexOfKeyName("data_type");
                    dataTypeMap.put(rowDetails.get(columnNameIndex), rowDetails.get(dataTypeIndex));
                } catch (ArrayIndexOutOfBoundsException e) {
                }
            }
            for (int i = 0; i < columns.length; i++) {
                System.out.println(dataTypeMap.get(columns[i]));
                String dataType = (String) dataTypeMap.get(columns[i]).trim();
                checkDataTypeWithValues(dataType, values[i]);
            }
        }
    }

    public void checkDataTypeWithValues(String dataType, String value) {
        String stripDataType = dataType.replaceAll("\\s+", "");
        String stripValue = value.replaceAll("\\s+", "");
        if (dataType.equals("String")) {
            if (stripValue.length() == 0) {
                System.out.println("Empty String");
            }
        } else if (stripDataType.equals("Integer")) {
            Integer i = Integer.parseInt(stripValue);
        } else if (stripDataType.equals("Float")) {
            float f = Float.parseFloat(value);
        } else if (stripDataType.equals("Boolean")) {
            if (stripValue.equals("true") || stripValue.equals("false")) {
            } else {
                System.out.println("Not a boolean!");
            }
        }
    }

    public boolean validateColumnCount(String[] columns, String[] values) {
        if (columns.length != values.length) {
            System.out.println("Column count and Values count does not match!");
            return false;
        }
        return true;
    }

    public void checkIfTableNameValid(String tableName) {

    }

    public static void main(String args[])
    {
        TableValidations tableValidations = new TableValidations("Schema", "DemoDB",
                new String[] {"Id", "Name"}, new String[] {"1", "lastname"});
        tableValidations.checkDataTypes();
    }
}

