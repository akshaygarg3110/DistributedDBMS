package database;

import java.io.BufferedReader;
import java.io.FileReader;
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
			
			if (query.contains("RECOVER")) {
				String[] recoverArray = query.split(" ");
				String filename = recoverArray[1] + "_dump.txt";
				executeRecovery("Database\\" + filename);
			} else {
				QueryParser queryParser = new QueryParser();
				boolean result = queryParser.parsingQuery(query, false);
			}

		}

	}

	public static void executeRecovery(String filePath) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(filePath));
			String line = reader.readLine();
			while (line != null) {
				if (line.length() > 0) {
					QueryParser queryParser = new QueryParser();
					boolean result = queryParser.parsingQuery(line, false);
				}
				line = reader.readLine();
			}
			reader.close();
		} catch (Exception e) {
			System.out.println("Error working with filesystem: " + e.getMessage());
		}

	}

}
