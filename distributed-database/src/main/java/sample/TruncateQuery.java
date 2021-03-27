package sample;

import com.opencsv.CSVWriter;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
        FileWriter fileWriter = new FileWriter(tempFile, true);
        BufferedReader tableReader = new BufferedReader(fileReader);
        BufferedWriter tableWriter = new BufferedWriter(fileWriter);
        //CSVWriter csvWriter = new CSVWriter(tableWriter,',',CSVWriter.NO_QUOTE_CHARACTER);

        String columnHeaders = tableReader.readLine();
        System.out.println(columnHeaders);
        tableWriter.write(columnHeaders);
        tableWriter.close();
        inputFile.delete();
        tempFile.renameTo(inputFile);
    }
}
