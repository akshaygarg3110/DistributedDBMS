package sample;

import database.QueryParser;

import java.io.IOException;

public class Main
{
    public static void main(String[] args) throws IOException {
        //InsertQuery insertQuery = new InsertQuery();
        //insertQuery.performInsertQueryOperation(new String[]{"8", "Gimmy", "54"});

        DeleteQuery obj = new DeleteQuery();
        obj.performDeleteQueryOperation(null,null);
    }

}
