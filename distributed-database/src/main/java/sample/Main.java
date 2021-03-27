package sample;

import java.io.IOException;

public class Main
{
    public static void main(String[] args) throws IOException {


        DeleteQuery deleteQuery = new DeleteQuery("Demo.csv","DemoDB");
        deleteQuery.performDeleteQueryOperation("Name", "Kong");

         /*
        TruncateQuery truncateQuery = new TruncateQuery("Demo.csv","DemoDB");
        truncateQuery.performTruncateQueryOperation();

   InsertQuery insertQuery = new InsertQuery("Demo.csv","DemoDB");
        //insert into demo(id,name,age) values(1,"",56)
        insertQuery.performInsertQueryOperation(new String[]{"2", "Kong", "23"});





 DropQuery dropQuery = new DropQuery("Demo.csv","DemoDB");
        dropQuery.performDropQueryOperation();

        */




    }

}
