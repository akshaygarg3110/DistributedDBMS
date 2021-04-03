package database;

import org.json.simple.JSONObject;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class CreateTableQuery {

    public void exceuteCreateTableQuery(String database, String tableName, String primaryKey,
                                        JSONObject tableColumnsObject, JSONObject tableForeignKeysObject) {
        boolean isFileCreated = createFileInFileSystem(database, tableName);
        if (isFileCreated) {
            createRecordInMetaDataFile(database, tableName, primaryKey, tableColumnsObject, tableForeignKeysObject);
        }
    }

    private void createRecordInMetaDataFile(String database, String tableName, String primaryKey,
                                            JSONObject tableColumnsObject, JSONObject tableForeignKeysObject) {
        try {
            File f = new File("meta.txt");
            FileWriter fstream = new FileWriter(f, true);
            BufferedWriter out = new BufferedWriter(fstream);
            String s = database + "@@@" + tableName + "@@@" + primaryKey + "@@@" + tableColumnsObject.toJSONString()
                    + "@@@" + tableForeignKeysObject.toJSONString();
            out.write(s);
            out.newLine();
            out.flush();
            out.close();
            RemoteFileHandler remoteFileHandler = new RemoteFileHandler("", "");
            remoteFileHandler.uploadObject();
        } catch (Exception e) {
            System.out.println("Exception while writing into file");
        }
    }

    private boolean createFileInFileSystem(String database, String tableName) {
        File tableFile = new File("Database\\" + database + "\\" + tableName + ".txt");
        if (!tableFile.exists()) {
            try {
                tableFile.createNewFile();
                RemoteFileHandler remoteFileHandler = new RemoteFileHandler(database, tableName);
                remoteFileHandler.uploadObject();
                tableFile.delete();
                return true;
            } catch (IOException e) {
                System.out.println(e);
                System.out.println("Exception while persisting data. Please contact admin.");
                return false;
            }
        } else {
            System.out.println("Table already exists");
            return false;
        }
    }

    public static void main(String[] args) {
        System.out.println("Started");
        CreateTableQuery CreateTableQuery = new CreateTableQuery();
        CreateTableQuery.createFileInFileSystem("faculty", "demo");
    }
}
