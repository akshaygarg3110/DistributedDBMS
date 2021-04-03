package database;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;


public class InsertQueryExecutor {
    private String tableName;
    private String databaseName;

    public InsertQueryExecutor(String tableName, String databaseName) {
        this.tableName = tableName;
        this.databaseName = databaseName;
    }
    public void performInsertQueryOperation(String[] values) throws IOException
    {
        try {
            File file = new File("Database" + "/" + databaseName + '/' + tableName + ".csv");
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
