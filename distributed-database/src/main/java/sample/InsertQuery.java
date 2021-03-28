package sample;

import java.io.*;
import java.util.*;


public class InsertQuery {
    private String tableName;
    private String databaseName;

    public InsertQuery(String tableName, String databaseName) {
        this.tableName = tableName;
        this.databaseName = databaseName;
    }

    public void performInsertQueryOperation(String[] values) throws IOException
    {
        try {
            File file = new File(databaseName + '/' + tableName);
            FileWriter fileWriter = new FileWriter(file, true);
            BufferedWriter tableWriter = new BufferedWriter(fileWriter);
            List<String> list = Arrays.asList(values);
            String dollarDelimited = String.join("$", list);
            tableWriter.newLine();
            tableWriter.write(dollarDelimited);
            tableWriter.close();
        }
        catch(IOException e) {
            e.getStackTrace();
        }
    }

}
