package Dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class staging_db {
    private static final String URL = "jdbc:mysql://localhost:3306/staging_db";
    private static final String USER = "root";
    private static final String PASSWORD = "123456";
    private static Connection connection = null;
    private static final control_db ctdb = new control_db();
    private static final Integer latestFileId = ctdb.getLatestFileId();

    // Phương thức lấy kết nối tới Staging Database
    public static Connection getConnection() throws ClassNotFoundException, SQLException {
        if (connection == null || connection.isClosed()) {
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
                connection = DriverManager.getConnection(URL, USER, PASSWORD);
            } catch (SQLException e) {
                throw e;
            }
        }
        return connection;
    }

    // Hàm cập nhật dữ liệu trong bảng tạm
    public void updateDataInTempTable() {
        String updateSizeSql = "UPDATE temp_product_daily " +
                               "SET size = 'N/A' " +
                               "WHERE size IS NULL OR TRIM(size) = ''";

        String updateDiscountSql = "UPDATE temp_product_daily " +
                                   "SET discount = ABS(discount) " +
                                   "WHERE discount < 0";

        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {

            // Thực hiện câu lệnh cập nhật size
            stmt.executeUpdate(updateSizeSql);

            // Thực hiện câu lệnh cập nhật discount
            stmt.executeUpdate(updateDiscountSql);

        } catch (SQLException | ClassNotFoundException e) {
            // Không thực hiện ghi log hoặc gửi email khi có lỗi
        }
    }

    // Tải dữ liệu từ file CSV vào bảng tạm
    public void loadCsvToTempTable() {
        // Truy cập vào file_path của file_config
        String filePath = ctdb.getLatestFilePath();
        String loadDataSql = "LOAD DATA INFILE '" + filePath + "' INTO TABLE temp_product_daily "
                + "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' "
                + "LINES TERMINATED BY '\\n' IGNORE 1 ROWS "
                + "(productID, productName, description, category, originalPrice, discountedPrice, discount, brand, size, rating, temp_date)";

        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {

            stmt.execute(loadDataSql);
        } catch (SQLException | ClassNotFoundException e) {
            // Không thực hiện ghi log hoặc gửi email khi có lỗi
        }
    }

    // Chuyển dữ liệu từ bảng tạm vào bảng chính
    public void insertDataFromTempToMain() {
        String insertDataSql = "INSERT INTO product_daily (productID, productName, description, category, originalPrice, discountedPrice, discount, brand, size, rating, date) "
                + "SELECT productID, productName, description, category, originalPrice, discountedPrice, discount, brand, size, rating, "
                + "STR_TO_DATE(temp_date, '%m/%d/%Y') AS date FROM temp_product_daily";

        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {

            stmt.executeUpdate(insertDataSql);
        } catch (SQLException | ClassNotFoundException e) {
            closeConnection();
            ctdb.closeConnection();
        }
    }

    // Đóng kết nối Staging Database
    public static void closeConnection() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            // Không thực hiện ghi log hoặc gửi email khi có lỗi
        }
    }
}
