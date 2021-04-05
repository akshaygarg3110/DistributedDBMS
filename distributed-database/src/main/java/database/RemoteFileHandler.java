package database;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import static java.nio.charset.StandardCharsets.UTF_8;

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


    public RemoteFileHandler(String directoryName, String fileName) {
        this.fileName = fileName;
        this.directoryName = directoryName;
    }

    void uploadObject() throws IOException {
        String filePath;
        String googleFilePath;
        if (directoryName.equalsIgnoreCase(DEFAULT_DATABASE_ROOT_PATH)) {
            filePath = DEFAULT_DATABASE_ROOT_PATH + "/" + fileName + ".txt";
            googleFilePath = DEFAULT_DATABASE_ROOT_PATH + "/" + directoryName +  "/" + fileName;
        } else {
            filePath = DEFAULT_DATABASE_ROOT_PATH + "/" + directoryName + "/" + fileName + ".txt";
            googleFilePath = fileName;
        }
        System.out.println(Paths.get(filePath));
        StorageOptions storageOptions = StorageOptions.newBuilder()
                .setProjectId(GOOGLE_PROJECT_ID)
                .setCredentials(GoogleCredentials.fromStream(new
                        FileInputStream("key.json"))).build();
        Storage storage = storageOptions.getService();
        BlobId blobId = BlobId.of(GOOGLE_BUCKET_NAME,
                googleFilePath);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();

        Blob blob = storage.get(blobId);
        if (blob != null) {
            byte[] prevContent = blob.getContent();
            System.out.println(new String(prevContent, UTF_8));
            WritableByteChannel channel = blob.writer();
            channel.write(ByteBuffer.wrap(Files.readAllBytes(Paths.get(filePath))));
            channel.close();
        } else {
            storage.create(blobInfo, Files.readAllBytes(Paths.get(filePath)));
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
            RemoteFileHandler remoteFileHandler = new RemoteFileHandler("dw", "testStudents19");
            remoteFileHandler.uploadObject();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
