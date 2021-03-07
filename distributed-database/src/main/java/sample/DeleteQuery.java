package sample;

import com.opencsv.CSVWriter;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class DeleteQuery {

    public void performDeleteQueryOperation(String column_name, String column_value) throws IOException
    {
        String rootPath = "/Users/preethz/Desktop";
        String dbName = "DemoDB";
        String tblName = "Demo.csv";
        String temp = "myFile2.txt";

        File inputFile = new File(rootPath + '/' + dbName + '/' + tblName);
        File tempFile = new File(rootPath + '/' + dbName + '/' + temp);

        BufferedReader tableReader = new BufferedReader(new FileReader(inputFile));
        BufferedWriter tableWriter = new BufferedWriter(new FileWriter(tempFile));
        CSVWriter csvWriter = new CSVWriter(tableWriter,',',CSVWriter.NO_QUOTE_CHARACTER);

        // DELETE from table_name WHERE column_name = column_value;
        // DELETE from Demo WHERE Age = 99;
        if ((column_name != null) && (column_value != null)) {
            try {
                String line = tableReader.readLine();
                int lineIndex = 0;
                int columnIndex = findIndex(line, column_name);
                System.out.println(columnIndex);
                List<String[]> records = new ArrayList<>();
                records.add(line.split(","));
                System.out.println(records);
                while ((line = tableReader.readLine()) != null) {
                    lineIndex++;
                    String[] columns = line.split(",");
                    if(columns[columnIndex].trim().equals(column_value)) {
                        System.out.println("Deleted " + columns[1] );
                        continue;
                    }
                    else {
                        records.add(columns);
                    }
                }
                csvWriter.writeAll(records);
                inputFile.delete();
                tempFile.renameTo(inputFile);
                csvWriter.close();
            }
            catch(IOException e){
                e.getStackTrace();
            }
        }
        else {
            String columnHeaders = tableReader.readLine();
            System.out.println(columnHeaders);

            List<String[]> list = new ArrayList<>();
            list.add(columnHeaders.split(","));

            csvWriter.writeAll(list);
            csvWriter.close();
            inputFile.delete();
            tempFile.renameTo(inputFile);
        }
    }

    public int findIndex(String line, String column_name){
        int columnIndex = 0;
        String[] columnHeaders = line.split(",");
        for(String value:columnHeaders) {
            if(value.trim().equals(column_name)) {
                break;
            }
            else {
                columnIndex++;
            }
        }
        return columnIndex;
    }
}

