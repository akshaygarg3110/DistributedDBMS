package sample;

import java.io.*;
import java.sql.Struct;
import java.util.*;

public class DeleteQuery {

    private String tableName;
    private String databaseName;

    public DeleteQuery(String tableName, String databaseName) {
        this.tableName = tableName;
        this.databaseName = databaseName;
    }

    public void performDeleteQueryOperation(String column_name, String column_value) throws IOException
    {
        String temp = "myFile2.csv";

        File inputFile = new File(databaseName + '/' + tableName);
        File tempFile = new File(databaseName + '/' + temp);

        BufferedReader tableReader = new BufferedReader(new FileReader(inputFile));
        BufferedWriter tableWriter = new BufferedWriter(new FileWriter(tempFile));

        // DELETE from table_name WHERE column_name = column_value;
        // DELETE from Demo WHERE Age = 99;
        if ((column_name != null) && (column_value != null)) {
            try {
                String line = tableReader.readLine();
                int lineIndex = 0;
                int columnIndex = findIndex(line, column_name);

                List<String[]> records = new ArrayList<>();
                records.add(line.split("\\$"));
                while ((line = tableReader.readLine()) != null) {
                    lineIndex++;
                    String[] columns = line.split("\\$");
                    if(columns[columnIndex].trim().equals(column_value)) {
                        System.out.println("Skipped " + columns[columnIndex]);
                        continue;
                    }
                    else{
                        System.out.println("Added " + columns[columnIndex]);
                        records.add(columns);
                    }
                }
                for(String[] i : records)
                {
                    tableWriter.write(String.join("$",i));
                    tableWriter.newLine();
                }
                tableWriter.close();
                inputFile.delete();
                tempFile.renameTo(inputFile);
            } catch (IOException e) {
                e.getStackTrace();
            }
        } else{
            String line = tableReader.readLine();

            List<String[]> records = new ArrayList<>();
            records.add(line.split("\\$"));
            for(String[] i : records)
            {
                tableWriter.write(String.join("$",i));
            }
            tableWriter.close();
            inputFile.delete();
            tempFile.renameTo(inputFile);
        }
    }

    public int findIndex(String line, String column_name){
        int columnIndex = 0;
        String[] columnHeaders = line.split("//$");
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


