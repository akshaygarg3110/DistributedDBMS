package database;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableValidations {

    private static final String DATABASE_ROOT_PATH = "Database";
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
        String tablePath = DATABASE_ROOT_PATH + "/" + this.databaseName + '/' + this.tableName + ".txt";
        return new BufferedReader(new FileReader(tablePath));
    }

    BufferedReader getMetaReader() throws Exception {
        String metaPath = DATABASE_ROOT_PATH + "/meta.txt";
        return new BufferedReader(new FileReader(metaPath));
    }


    public String[] getColumns() {
        try {
            BufferedReader metaReader = getMetaReader();
            String rows;
            System.out.println(tableName);
            while ((rows = metaReader.readLine()) != null) {
                String[] row = rows.split("@@@");
                if (row[2].equalsIgnoreCase(this.tableName)) {
                    System.out.println(row);
                    return row;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
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

    public boolean checkPrimaryKey(String actualTableName) {
        // selecting all records of Schema table
        SelectQueryExecutor selectQueryExecutor = new SelectQueryExecutor(tableName, databaseName);
        Map<Integer, List<String>> resultSet = selectQueryExecutor.executeSelectStatementWithFullColumns();

        // position of primary_key column in schema file
        int indexOfKey = getIndexOfKeyName("primary_key");
        String columnName = getPrimaryKey();
        // looping the schema table records
        for (Map.Entry<Integer, List<String>> rows : resultSet.entrySet()) {
            try {
                List<String> rowDetails = rows.getValue();
                if (columnName != null) {
                    SelectQueryExecutor selectQueryExecutorActual = new SelectQueryExecutor(actualTableName, databaseName);
                    List<String> list = new ArrayList<>();
                    list.add(columnName);
                    selectQueryExecutorActual.setFieldList(list);
                    Map<Integer, List<String>> resultSetActual = selectQueryExecutorActual.executeSelectStatementWithColumnList();
                    for (List<String> i : resultSetActual.values()) {
                        if (i.get(0).equals(inputFieldMap.get(columnName))) {
                            System.out.println("Returning false");
                            return false;
                        }
                    }
                }
            } catch (ArrayIndexOutOfBoundsException e) {
            }
        }
        return true;
    }

    private String getPrimaryKey() {
        if (getColumns() != null) {
            return getColumns()[3];
        }
        return null;
    }

    private String getForeignKey() {
        if (getColumns() != null) {
            return getColumns()[5];
        }
        return null;
    }

    public boolean checkForeignKey(String actualTable) {
        SelectQueryExecutor selectQueryExecutor = new SelectQueryExecutor(tableName, databaseName);
        Map<Integer, List<String>> resultSet = selectQueryExecutor.executeSelectStatementWithFullColumns();

        String foreignKey = getForeignKey();
        if (foreignKey != null) {
            JSONObject foreignKeyObj = new JSONObject(foreignKey);
            int indexOfKey = getIndexOfKeyName("foreign_key");
            JSONArray foreignKeyArrays = new JSONArray(foreignKeyObj.get("keys").toString());
            for (int fk = 0; fk < foreignKeyArrays.length(); fk++) {
                JSONObject foreignKeyConstraintObj = new JSONObject(foreignKeyArrays.get(fk).toString());
                String columnName = foreignKeyConstraintObj.getString("columns");
                String foreignKeyColumnName = foreignKeyConstraintObj.getString("foreignKeyColumn");
                String foreignKeyTableName = foreignKeyConstraintObj.getString("foreignKeyTable");
                for (Map.Entry<Integer, List<String>> rows : resultSet.entrySet()) {
                    try {
                        List<String> rowDetails = rows.getValue();
                        if (Boolean.parseBoolean(rowDetails.get(indexOfKey))) {
                            int columnNameIndex = getIndexOfKeyName("column_name");
                            int foreignKeyColumnNameIndex = getIndexOfKeyName("foreign_key_column_name");
                            int foreignKeyTableIndex = getIndexOfKeyName("foreign_key_table_name");
                            String foreignKeyValue = inputFieldMap.get(columnName);

                            List<String> list = new ArrayList<String>();
                            list.add(foreignKeyColumnName);
                            // TODO: foreignKeyTableName cannot have white spaces.
                            SelectQueryExecutor dependentSelectQueryExecutor = new SelectQueryExecutor(foreignKeyTableName, databaseName);
                            dependentSelectQueryExecutor.setFieldList(list);
                            Map<Integer, List<String>> dependentResultSet = dependentSelectQueryExecutor.executeSelectStatementWithColumnList();

                            for (List<String> i : dependentResultSet.values()) {
                                if (i.get(0).equals(foreignKeyValue)) {
                                    System.out.println("Good to insert");
                                    return true;
                                }
                            }
                        }
                    } catch (ArrayIndexOutOfBoundsException e) {
                        System.out.println("Foreign key violation");
                    }
                }
            }
        } else {
            System.out.println("No foreign key constraint");
            return true;
        }
        System.out.println("Foreign key violation");
        return false;
    }

    public boolean checkDataTypes() {
        SelectQueryExecutor selectQueryExecutor = new SelectQueryExecutor(tableName, databaseName);
        Map<Integer, List<String>> resultSet = selectQueryExecutor.executeSelectStatementWithFullColumns();
        boolean result = false;
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
                String dataType = (String) dataTypeMap.get(columns[i]).trim();
                result = checkDataTypeWithValues(dataType, values[i]);
                if (result == false) {
                    break;
                }
            }
        }
        return result;
    }

    public boolean checkDataTypeWithValues(String dataType, String value) {
        String stripDataType = dataType.replaceAll("\\s+", "");
        String stripValue = value.replaceAll("\\s+", "");
        System.out.println(dataType);
        System.out.println(stripValue);
        if (dataType.equals("String")) {
            if (stripValue.length() == 0) {
                System.out.println("Empty String");
                return false;
            }
        } else if (stripDataType.equals("Integer")) {
            Integer i = Integer.parseInt(stripValue);
        } else if (stripDataType.equals("Float")) {
            float f = Float.parseFloat(value);
        } else if (stripDataType.equals("Boolean")) {
            if (stripValue.equals("true") || stripValue.equals("false")) {
            } else {
                System.out.println("Not a boolean!");
                return false;
            }
        }
        return true;
    }

    public boolean validateColumnCount(String[] columns, String[] values) {
        if (columns.length != values.length) {
            System.out.println("Column count and Values count does not match!");
            return false;
        }
        return true;
    }

    public boolean checkIfTableNameValid(String tableName) {
        List<String> tableNames = getTableNames();
        for(String table : tableNames) {
            if (table.equalsIgnoreCase(tableName)) {
                System.out.println("Table already exists");
                return false;
            }
        }
        return true;
    }

    private List<String> getTableNames() {
        List<String> tableList = new ArrayList<>();
        try {
            BufferedReader metaReader = getMetaReader();
            String rows;
            while ((rows = metaReader.readLine()) != null) {
                String[] row = rows.split("@@@");
                tableList.add(row[2]);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return tableList;
    }

    public static void main(String args[]) {
        TableValidations tableValidations = new TableValidations("students", "test",
                new String[]{"Id", "Name"}, new String[]{"1", "lastname"});
        tableValidations.checkPrimaryKey("students");
    }
}

