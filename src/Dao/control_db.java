package Dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import SetUp.HostingerEmail;

public class control_db {
    private static final String URL = "jdbc:mysql://localhost:3306/control_db";
    private static final String USER = "root";
    private static final String PASSWORD = "123456";
    private static Connection connection = null;
    private static final String email = "20130374@st.hcmuaf.edu.vn";

    // Phương thức lấy kết nối tới Control Database
    public static Connection getConnection() throws ClassNotFoundException, SQLException {
        if (connection == null || connection.isClosed()) {
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
                connection = DriverManager.getConnection(URL, USER, PASSWORD);
            } catch (SQLException e) {
            	//3.2 Gửi email thông báo lỗi
                HostingerEmail.sendEmail(email, "Kết nối Control Database", "Kết nối thất bại");
                throw e;
            }
        }
        return connection;
    }
    //3.1 Xác Minh path Mới nhất trong Control Database(file_config)
    // Lấy đường dẫn file tạm mới nhất
    public static String getLatestFilePathTemp() {
        return getLatestFilePath("file_path_temp");
    }

    // Lấy đường dẫn file chính mới nhất
    public static String getLatestFilePath() {
        return getLatestFilePath("file_path");
    }

    // Phương thức lấy đường dẫn file mới nhất
    private static String getLatestFilePath(String column) {
        String query = "SELECT " + column + " FROM file_config ORDER BY created_date DESC LIMIT 1";
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {
            return rs.next() ? rs.getString(column) : null;
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    // Lấy ID file mới nhất từ file_config
    public static Integer getLatestFileId() {
        String query = "SELECT MAX(file_id) AS latest_id FROM file_config";
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             PreparedStatement stmt = conn.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {
            return rs.next() ? rs.getInt("latest_id") : null;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    // Ghi log vào bảng logs
    public static void logToDatabase(Integer fileId, String processStep, String status, String logMessage) {
        String query = "INSERT INTO logs (file_id, process_step, status, log_message) VALUES (?, ?, ?, ?)";
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setObject(1, fileId);
            stmt.setString(2, processStep);
            stmt.setString(3, status);
            stmt.setString(4, logMessage);
            int rowsAffected = stmt.executeUpdate();
            System.out.println(rowsAffected > 0 ? "Ghi log thành công!" : "Không có dòng nào được ghi.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //24. Đóng kết nối Control Database
    public static void closeConnection() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
      
}
}
