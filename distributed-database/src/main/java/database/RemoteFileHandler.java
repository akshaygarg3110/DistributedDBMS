package database;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

public class RemoteFileHandler {

    /**
     * ref: https://cloud.google.com/storage/docs
     */

    public static final String DEFAULT_DATABASE_ROOT_PATH = "Database";
    public static final String GOOGLE_BUCKET_NAME = "5408_project_team6";
    public static final String GOOGLE_PROJECT_ID = "csci5408-w21";
    public static final String REMOTE_URL = "https://storage.googleapis.com/5408_project_team6/";

    private final String directoryName;
    private final String fileName;

    private BufferedReader reader;

    public RemoteFileHandler(String directoryName, String fileName) {
        this.fileName = fileName;
        this.directoryName = directoryName;
    }

    void uploadObject() throws IOException {
        String filePath;
        String googleFilePath;
        if (directoryName.equalsIgnoreCase(DEFAULT_DATABASE_ROOT_PATH)) {
            filePath = DEFAULT_DATABASE_ROOT_PATH + "/" + fileName + ".txt";
            googleFilePath = DEFAULT_DATABASE_ROOT_PATH + "/" + fileName;
        } else {
            filePath = DEFAULT_DATABASE_ROOT_PATH + "/" + directoryName + "/" + fileName + ".txt";
            googleFilePath = DEFAULT_DATABASE_ROOT_PATH + "/" + directoryName + "/" + fileName;
        }
        StorageOptions storageOptions = StorageOptions.newBuilder()
                .setProjectId(GOOGLE_PROJECT_ID)
                .setCredentials(GoogleCredentials.fromStream(new
                        FileInputStream("key.json"))).build();
        Storage storage = storageOptions.getService();
        BlobId blobId = BlobId.of(GOOGLE_BUCKET_NAME,
                googleFilePath);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        storage.create(blobInfo, Files.readAllBytes(Paths.get(filePath)));
    }

    public BufferedReader getReader() {
        try {
            String urlPath = REMOTE_URL
                    + DEFAULT_DATABASE_ROOT_PATH + "/"
                    + directoryName + "/"
                    + fileName;
            URL url = new URL(urlPath);
            reader = new BufferedReader(
                    new InputStreamReader(url.openStream()));
            return reader;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public void closeReader() {
        try {
            if (reader != null) {
                reader.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void deleteObject() throws IOException {
        String googleFilePath;
        if (directoryName.equalsIgnoreCase(DEFAULT_DATABASE_ROOT_PATH)) {
            googleFilePath = DEFAULT_DATABASE_ROOT_PATH + "/" + fileName;
        } else {
            googleFilePath = DEFAULT_DATABASE_ROOT_PATH + "/" + directoryName + "/" + fileName;
        }

        StorageOptions storageOptions = StorageOptions.newBuilder()
                .setProjectId(GOOGLE_PROJECT_ID)
                .setCredentials(GoogleCredentials.fromStream(new
                        FileInputStream("key.json"))).build();
        Storage storage = storageOptions.getService();
        storage.delete(GOOGLE_BUCKET_NAME, googleFilePath);
    }

    public static void main(String[] args) {
        try {
            RemoteFileHandler remoteFileHandler = new RemoteFileHandler("test", "students");
            remoteFileHandler.uploadObject();
            String line;
            BufferedReader reader = remoteFileHandler.getReader();
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
            remoteFileHandler.closeReader();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
