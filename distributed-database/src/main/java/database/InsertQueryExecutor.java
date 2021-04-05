package database;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class InsertQueryExecutor {
    private String tableName;
    private String databaseName;
    private String location;

    public InsertQueryExecutor(String tableName, String databaseName, String location) {
        this.tableName = tableName;
        this.databaseName = databaseName;
        this.location = location;
    }
    public void performInsertQueryOperation(String[] values) throws IOException
    {
        try {
            File file = new File("Database" + "/" + databaseName + '/' + tableName + ".txt");
            FileWriter fileWriter = new FileWriter(file, true);
            BufferedWriter tableWriter = new BufferedWriter(fileWriter);
            List<String> list = Arrays.asList(values);
            tableWriter.write("\n");
            tableWriter.write(String.join("$", list));
            tableWriter.close();
            RemoteFileHandler rhf = new RemoteFileHandler(databaseName, tableName);
            rhf.uploadObject();
        }
        catch(IOException e) {
            e.getStackTrace();
        }
    }
}
