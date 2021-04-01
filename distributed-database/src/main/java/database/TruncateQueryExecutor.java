package database;

import java.io.*;

public class TruncateQueryExecutor {
    private String databaseName;

    public TruncateQueryExecutor(String databaseName) {
        this.databaseName = databaseName;
    }
    public void performTruncateQueryOperation(String tableName) throws IOException {

        String temp = "myFile2.txt";

        File inputFile = new File( databaseName + '/' + tableName + ".csv");
        File tempFile = new File( databaseName + '/' + temp);

        FileReader fileReader = new FileReader(inputFile);
        FileWriter fileWriter = new FileWriter(tempFile);
        BufferedReader tableReader = new BufferedReader(fileReader);
        BufferedWriter tableWriter = new BufferedWriter(fileWriter);

        String columnHeaders = tableReader.readLine();
        tableWriter.write(columnHeaders);
        tableWriter.close();
        inputFile.delete();
        tempFile.renameTo(inputFile);
        System.out.println("Table values truncated");
    }
}
