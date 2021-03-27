package database;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;


public class TransactionManager {

    private static final String BEGIN = "BEGIN TRANSACTION";
    private static final String ROLLBACK = "ROLLBACK";
    private static final String SAVEPOINT = "SAVEPOINT";
    private static final String COMMIT = "COMMIT";



    long transactionId;
    String databaseName;

    List<String> transactionList;

    public TransactionManager(String databaseName) {
        transactionId = System.currentTimeMillis();
        this.databaseName = databaseName;
    }


    void addTransaction(String transactionStatement) {
        try {
            FileWriter tempTransWriter = new FileWriter(databaseName + "/" + String.valueOf(transactionId));
            tempTransWriter.write(transactionStatement);
            tempTransWriter.write("\n");
            tempTransWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    void endAndExecuteTransaction() {
        try {
            BufferedReader metaReader = new BufferedReader(new FileReader(databaseName + "/" + String.valueOf(transactionId)));
            String rows = null;
            transactionList = new ArrayList<>();
            while ((rows = metaReader.readLine()) != null) {
                transactionList.add(rows);
            }
            List<String> queryToBeExecutedList = queryThatIsExecutedInTransaction(transactionList);
            //send the list of executable queries to parser to tokenize and execute by corresponding executor.
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private List<String> queryThatIsExecutedInTransaction(List<String> transactionList) {
        List<String> executableQueries = new ArrayList<>();
        int currentSavePointIndex = -1;
        for (String query :  transactionList) {
            if (query.contains(SAVEPOINT)) {
                executableQueries.add(SAVEPOINT);
                currentSavePointIndex = executableQueries.size();
            }
            else if (query.contains(ROLLBACK)) {
                if (currentSavePointIndex != -1) {
                    int lastIndexOfExecutableQuery = executableQueries.size()-1;
                    while (!executableQueries.get(lastIndexOfExecutableQuery).equalsIgnoreCase(SAVEPOINT)) {
                        executableQueries.remove(lastIndexOfExecutableQuery);
                        lastIndexOfExecutableQuery--;
                    }
                    executableQueries.remove(lastIndexOfExecutableQuery);
                    currentSavePointIndex = -1;
                } else {
                    throw new IllegalArgumentException("Improper Rollback occurred");
                }
            }
            else if(query.contains(COMMIT)) {
                break;
            }
            else {
                if(!query.equalsIgnoreCase(BEGIN))
                    executableQueries.add(query);
            }
        }
        if (currentSavePointIndex != -1) {
            executableQueries.remove(SAVEPOINT);
        }
        return executableQueries;
    }

}
