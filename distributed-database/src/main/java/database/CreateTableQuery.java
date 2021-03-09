package database;

import java.io.File;
import java.io.IOException;

import org.json.simple.JSONObject;

public class CreateTableQuery {

	public void exceuteCreateTableQuery(String database, String tableName, String primaryKey, JSONObject tableColumnsObject,
			JSONObject tableForeignKeysObject) {
		
		createCsvFileInFileSystem(database, tableName);
		
		createRecordInMetaDataFile(database, tableName, primaryKey, tableColumnsObject, tableForeignKeysObject);
	}
	
	private void createRecordInMetaDataFile(String database, String tableName, String primaryKey, JSONObject tableColumnsObject,
			JSONObject tableForeignKeysObject ) {
		
	}
	
	private void createCsvFileInFileSystem(String database, String tableName) {
		File tableFile = new File("Database\\"+ database + "\\" + tableName + ".csv");
		if(!tableFile.exists()){
			try {
				tableFile.createNewFile();
			} catch (IOException e) {
				System.out.println(e);
				System.out.println("Exception while persisting data. Please contact admin.");
			}
		}else{
		  System.out.println("Table already exists");
		}
	}
	
	public static void main(String[] args) {
		System.out.println("Started");
		CreateTableQuery CreateTableQuery = new CreateTableQuery();
		CreateTableQuery.createCsvFileInFileSystem("test1", "file");	
	}
}
