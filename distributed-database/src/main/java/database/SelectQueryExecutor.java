package database;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

public class SelectQueryExecutor {
    private String tableName;
    private String databaseName;
    private Map<String, Object> constraintMap;
    private List<String> fieldList;
    private Map<Integer, String> fieldMap;
    private Map<String, String> rowMap;

    public SelectQueryExecutor(String tableName, String databaseName) {
        this.tableName = tableName;
        this.databaseName = databaseName;
    }

    public void setFieldList(List<String> fieldList) {
        this.fieldList = fieldList;
    }

    public void setConstraintMap(Map<String, Object> constraintMap) {
        this.constraintMap = constraintMap;
    }

    void populateColumnMap() {
        try {
            String tablePath = this.databaseName + '/' + this.tableName;
            BufferedReader tableReader = new BufferedReader(new FileReader(tablePath));
            String rows = "";
            rows = tableReader.readLine();
            String[] columns = rows.split(",");
            fieldMap = new HashMap<>();
            for( int i = 0; i < columns.length; i++) {
                fieldMap.put(i, columns[i]);
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }


    Map<Integer, List<String>> executeSelectStatementWithFullColumns() {
        try {
            Map<Integer, List<String>> selectResult = new HashMap<>();
            String tablePath = this.databaseName + '/' + this.tableName;
            BufferedReader tableReader = new BufferedReader(new FileReader(tablePath));
            String rows = tableReader.readLine();
            int rowCounter = 1;
            while((rows = tableReader.readLine()) != null) {
                String[] values = rows.split(",");
                selectResult.put(rowCounter, Arrays.asList(values));
                rowCounter++;
            }
            return selectResult;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    Map<Integer, List<String>> executeSelectStatementWithColumnList() {
        populateColumnMap();
        List<Integer> filteredIndex = new ArrayList<>();
        for(Map.Entry<Integer, String> column: fieldMap.entrySet()) {
            if (fieldList.contains(column.getValue())) {
                filteredIndex.add(column.getKey());
            }
        }
        Map<Integer, List<String>> selectResult = executeSelectStatementWithFullColumns();
        if (selectResult != null) {
            for (Map.Entry<Integer, List<String>> row : selectResult.entrySet()) {
                List<String> filteredColumnValues = new ArrayList<>();
                for (int i = 0; i < row.getValue().size(); i++) {
                    if (filteredIndex.contains(i)) {
                        filteredColumnValues.add(row.getValue().get(i));
                    }
                }
                row.setValue(filteredColumnValues);
            }
            return selectResult;
        } else {
            return null;
        }
    }

    Map<Integer, List<String>> executeSelectStatementWithConstraint() {
        return null;
    }


}
