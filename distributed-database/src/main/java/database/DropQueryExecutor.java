package database;

import java.io.*;

public class DropQueryExecutor {
    private String databaseName;

    public DropQueryExecutor(String databaseName){
        this.databaseName = databaseName;
    }

    public void performDropQueryOperation(String tableName) throws IOException {
        File inputFile = new File(databaseName + '/' + tableName + ".txt");
        if (inputFile.exists()) {
            try {
                inputFile.delete();
                System.out.println("Table dropped");
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
        else{
            System.out.println("Table doesn't exists");
        }
    }
}