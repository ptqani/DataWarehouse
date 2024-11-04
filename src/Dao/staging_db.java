package Dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import SetUp.HostingerEmail;

public class staging_db {
    private static final String URL = "jdbc:mysql://localhost:3306/staging_db";
    private static final String USER = "root";
    private static final String PASSWORD = "123456";
    private static Connection connection = null;
    private static final String email = "20130374@st.hcmuaf.edu.vn";
    private static final control_db ctdb = new control_db();
    private static final Integer latestFileId = ctdb.getLatestFileId();

    // Phương thức lấy kết nối tới Staging Database
    public static Connection getConnection() throws ClassNotFoundException, SQLException {
        if (connection == null || connection.isClosed()) {
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
                connection = DriverManager.getConnection(URL, USER, PASSWORD);
            } catch (SQLException e) {
            	//8.2 Gửi email thông báo lỗi
                HostingerEmail.sendEmail(email, "Kết nối Staging Database", "Kết nối thất bại");
                ctdb.closeConnection();
                throw e;
            }
        }
        return connection;
    }
 
    // Tải dữ liệu từ file CSV vào bảng tạm
    public void loadCsvToTempTable() {
        //8.1. Truy cập vào file_path của file_config
        String filePath = ctdb.getLatestFilePath();
        String loadDataSql = "LOAD DATA INFILE '" + filePath + "' INTO TABLE temp_product_daily "
                + "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' "
                + "LINES TERMINATED BY '\\n' IGNORE 1 ROWS "
                + "(productID, productName, description, category, originalPrice, discountedPrice, discount, brand, size, rating, temp_date)";

        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {

            stmt.execute(loadDataSql);
            System.out.println("Dữ liệu đã được tải vào bảng tạm.");
        } catch (SQLException | ClassNotFoundException e) {
            System.out.println("Lỗi khi tải dữ liệu vào bảng tạm: " + e.getMessage());
            e.printStackTrace();
        }
    }
    //10.1  Lưu dữ liệu vào db staging trong Product_daily
    // Chuyển dữ liệu từ bảng tạm vào bảng chính
    public void insertDataFromTempToMain() {
        String insertDataSql = "INSERT INTO product_daily (productID, productName, description, category, originalPrice, discountedPrice, discount, brand, size, rating, date) "
                + "SELECT productID, productName, description, category, originalPrice, discountedPrice, discount, brand, size, rating, "
                + "STR_TO_DATE(temp_date, '%m/%d/%Y') AS date FROM temp_product_daily";

        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {

            int rowsAffected = stmt.executeUpdate(insertDataSql);
            System.out.println("Đã chuyển " + rowsAffected + " dòng dữ liệu vào bảng chính.");
            //11. Cập nhật trạng thái trong logs "Success Extracting"
            ctdb.logToDatabase(latestFileId, "Extract", "Success Extracting", "Lưu dữ liệu thành công");
        } catch (SQLException | ClassNotFoundException e) {
            System.out.println("Lỗi khi chuyển dữ liệu vào bảng chính: " + e.getMessage());
            //10.2 Ghi vào bảng logs với trạng thái "Failed"
            ctdb.logToDatabase(latestFileId, "Extract", "Failed", "Lưu dữ liệu thất bại");
            closeConnection();
            ctdb.closeConnection();
            e.printStackTrace();
        }
    }
  //13.3 | 23 Đóng kết nối Staging Database
    public static void closeConnection() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
}
}
