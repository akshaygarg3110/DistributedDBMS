package sample;

import java.io.*;

public class TruncateQuery {
    private String tableName;
    private String databaseName;

    public TruncateQuery(String tableName, String databaseName) {
        this.tableName = tableName;
        this.databaseName = databaseName;
    }
    public void performTruncateQueryOperation() throws IOException {

        String temp = "myFile2.txt";

        File inputFile = new File( databaseName + '/' + tableName);
        File tempFile = new File( databaseName + '/' + temp);

        FileReader fileReader = new FileReader(inputFile);
        FileWriter fileWriter = new FileWriter(tempFile);
        BufferedReader tableReader = new BufferedReader(fileReader);
        BufferedWriter tableWriter = new BufferedWriter(fileWriter);

        String columnHeaders = tableReader.readLine();
        System.out.println(columnHeaders);
        tableWriter.write(columnHeaders);
        tableWriter.close();
        inputFile.delete();
        tempFile.renameTo(inputFile);
    }
}
