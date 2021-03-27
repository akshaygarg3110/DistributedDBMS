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
            List<String[]> list = new ArrayList<>();
            list.add(values);
            String a = null;
            for(String[] temp : list)
            {
                a = Arrays.toString(temp);
                System.out.println(a);
            }
            String arr = a.substring(1, a.length() - 1 );
            System.out.println(arr);
            tableWriter.newLine();
            tableWriter.write(arr);
            tableWriter.close();
        }
        catch(IOException e) {
            e.getStackTrace();
        }
    }

}
