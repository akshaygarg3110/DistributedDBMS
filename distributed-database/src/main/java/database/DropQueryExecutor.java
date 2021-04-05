package database;

import java.io.*;

public class DropQueryExecutor {
    private String databaseName;
    private String tableName;
    private String location;

    public DropQueryExecutor(String tableName, String databaseName, String location){
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.location = location;
    }

    public void performDropQueryOperation() throws IOException {
        File inputFile = new File("Database/" + databaseName + '/' + tableName + ".txt");
        if (inputFile.exists()) {
            try {
                inputFile.delete();
                System.out.println("Table dropped");

                if(location.equalsIgnoreCase("remote")){
                    RemoteFileHandler rhf = new RemoteFileHandler(databaseName, tableName);
                    rhf.deleteObject();
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
        else{
            System.out.println("Table doesn't exists");
        }
    }
}