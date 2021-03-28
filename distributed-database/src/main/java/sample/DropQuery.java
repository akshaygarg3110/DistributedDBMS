package sample;

import java.io.*;

public class DropQuery {
    private String tableName;
    private String databaseName;

    public DropQuery(String tableName, String databaseName) {
        this.tableName = tableName;
        this.databaseName = databaseName;
    }

    public void performDropQueryOperation() throws IOException {
        File inputFile = new File(databaseName + '/' + tableName);
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