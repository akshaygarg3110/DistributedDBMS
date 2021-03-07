package sample;

import com.opencsv.CSVWriter;
import java.io.*;
import java.util.*;


public class InsertQuery {

    public void performInsertQueryOperation(String[] values) throws IOException
    {
        try {
            String rootPath = "/Users/preethz/Desktop";
            String dbName = "DemoDB";
            String tableName = "Demo.csv";

            File file = new File(rootPath + '/' + dbName + '/' + tableName);
            BufferedWriter tableWriter = new BufferedWriter(new FileWriter(file, true));
            CSVWriter csvWriter = new CSVWriter(tableWriter, ',', CSVWriter.NO_QUOTE_CHARACTER);

            List<String[]> list = new ArrayList<>();
            list.add(values);
            csvWriter.writeAll(list);
            csvWriter.close();
        }
        catch(IOException e) {
            e.getStackTrace();
        }
    }

}
