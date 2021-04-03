package database;

import java.util.Scanner;

public class Main {

    public static void main(String[] args) {

        Scanner scanner = new Scanner(System.in);

        LoginUI loginUI = new LoginUI();
        loginUI.manageLoginUI(scanner);


        String query = "";
        boolean isExitQuery = false;

        while (!isExitQuery) {
            System.out.println("Enter your query or use exit command to exit from the database management system.");
            query = scanner.nextLine();

            if (query == null) {
                query = "";
            }

            if (query.trim().equalsIgnoreCase("exit")) {
                isExitQuery = true;
                continue;
            }

            QueryParser queryParser = new QueryParser();
            queryParser.parsingQuery(query, false);
        }

    }

}
