package transform;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import com.opencsv.CSVReader;

public class DatabaseCSVHandler {
	private static Connection stagingConnection = null;
	private static Connection controlConnection = null;

	// Thông tin kết nối
	private static final String STAGING_URL = "jdbc:mysql://localhost:3306/staging_db";
	private static final String CONTROL_URL = "jdbc:mysql://localhost:3306/control_db";
	private static final String USER = "root";
	private static final String PASSWORD = "Chihai000@";

	// Đường dẫn file CSV
	private static final String CSV_FILE_PATH = "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\products.csv";

	// Kết nối tới database staging
	public static Connection getStagingConnection() throws SQLException {
		if (stagingConnection == null || stagingConnection.isClosed()) {
			stagingConnection = DriverManager.getConnection(STAGING_URL, USER, PASSWORD);
			System.out.println("Connected to staging database successfully.");
		}
		return stagingConnection;
	}

	// Kết nối tới database control
	public static Connection getControlConnection() throws SQLException {
		if (controlConnection == null || controlConnection.isClosed()) {
			controlConnection = DriverManager.getConnection(CONTROL_URL, USER, PASSWORD);
			System.out.println("Connected to control database successfully.");
		}
		return controlConnection;
	}

	// Hàm làm sạch dữ liệu
	private static String cleanText(String value) {
		if (value == null || value.trim().isEmpty()) {
			return "NA"; // Thay thế giá trị null hoặc rỗng
		}
		return value.trim().replaceAll("\\s+", " "); // Loại bỏ khoảng trắng dư thừa
	}

	private static String cleanNumeric(String value) {
		if (value == null || value.trim().isEmpty()) {
			return "NA"; // Thay thế giá trị null hoặc rỗng
		}
		return value.replaceAll("[^0-9.]", ""); // Loại bỏ ký tự không phải số
	}

	public static void main(String[] args) {
		try (Connection stagingConn = getStagingConnection()) {
			File csvFile = new File(CSV_FILE_PATH);
			if (!csvFile.exists()) {
				throw new FileNotFoundException("CSV file not found: " + CSV_FILE_PATH);
			}

			// Đọc file CSV và chèn vào bảng tạm trong staging_db
			try (CSVReader csvReader = new CSVReader(new FileReader(CSV_FILE_PATH));
					PreparedStatement pstmt = stagingConn.prepareStatement(
							"INSERT INTO temp_product_daily (productID, productName, category, originalPrice, discountedPrice, discount, brand, size, rating, date) "
									+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {

				String[] nextLine;
				while ((nextLine = csvReader.readNext()) != null) {
					pstmt.setString(1, cleanText(nextLine[0])); // Làm sạch productID
					pstmt.setString(2, cleanText(nextLine[1])); // Làm sạch productName
					pstmt.setString(3, cleanText(nextLine[2])); // Làm sạch category
					pstmt.setString(4, cleanNumeric(nextLine[3])); // Làm sạch originalPrice
					pstmt.setString(5, cleanNumeric(nextLine[4])); // Làm sạch discountedPrice
					pstmt.setString(6, cleanNumeric(nextLine[5])); // Làm sạch discount
					pstmt.setString(7, cleanText(nextLine[6])); // Làm sạch brand
					pstmt.setString(8, cleanText(nextLine[7])); // Làm sạch size
					pstmt.setString(9, cleanNumeric(nextLine[8])); // Làm sạch rating
					pstmt.setString(10, cleanText(nextLine[9])); // Làm sạch date
					pstmt.addBatch();
				}
				pstmt.executeBatch();
				System.out.println("Data inserted into temp_product_daily.");
			}

			// Làm sạch dữ liệu và chèn vào bảng chính trong staging_db
			try (Statement stmt = stagingConn.createStatement()) {
				String cleanDataQuery = "INSERT INTO product_daily (productID, productName, category, originalPrice, discountedPrice, discount, brand, size, rating, date) "
						+ "SELECT productID, productName, category, originalPrice, discountedPrice, discount, brand, size, rating, date FROM temp_product_daily";
				stmt.executeUpdate(cleanDataQuery);
				System.out.println("Data cleaned and inserted into product_daily.");
			}

			// Ghi log thành công vào control_db
			try (Connection controlConn = getControlConnection();
					PreparedStatement logStmt = controlConn
							.prepareStatement("INSERT INTO logs (status, message) VALUES (?, ?)")) {
				logStmt.setString(1, "Success");
				logStmt.setString(2, "Data successfully processed.");
				logStmt.executeUpdate();
			}

		} catch (Exception e) {
			e.printStackTrace();

			// Ghi log thất bại vào control_db
			try (Connection controlConn = getControlConnection();
					PreparedStatement logStmt = controlConn
							.prepareStatement("INSERT INTO logs (status, message) VALUES (?, ?)")) {
				logStmt.setString(1, "Failed");
				logStmt.setString(2, e.getMessage());
				logStmt.executeUpdate();
			} catch (SQLException logEx) {
				logEx.printStackTrace();
			}
		}
	}
}
