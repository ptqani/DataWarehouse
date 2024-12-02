package Dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class control_db {
    private static final String URL = "jdbc:mysql://localhost:3306/control_db";
    private static final String USER = "root";
    private static final String PASSWORD = "123456";
    private static Connection connection = null;

    // Phương thức lấy kết nối tới Control Database
    public static Connection getConnection() {
        try {
            if (connection == null || connection.isClosed()) {
                // Đảm bảo driver được tải
                Class.forName("com.mysql.cj.jdbc.Driver");
                // Kết nối cơ sở dữ liệu
                connection = DriverManager.getConnection(URL, USER, PASSWORD);
            }
        } catch (ClassNotFoundException | SQLException e) {
            // Ghi thông báo lỗi mà không in ra stack trace
            System.err.println("Lỗi kết nối cơ sở dữ liệu: " + e.getMessage());
        }
        return connection;
    }

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
        } catch (SQLException e) {
            // Ghi thông báo lỗi mà không in ra stack trace
            System.err.println("Lỗi khi thực thi truy vấn để lấy đường dẫn file mới nhất: " + e.getMessage());
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
            // Ghi thông báo lỗi mà không in ra stack trace
            System.err.println("Lỗi khi thực thi truy vấn để lấy ID file mới nhất: " + e.getMessage());
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
            // Nếu không có dòng nào bị ghi, thông báo lỗi
            if (rowsAffected <= 0) {
                System.err.println("Không có dòng nào được ghi vào bảng logs.");
            }
        } catch (SQLException e) {
            // Ghi thông báo lỗi mà không in ra stack trace
            System.err.println("Lỗi khi ghi log vào cơ sở dữ liệu: " + e.getMessage());
        }
    }

    // Đóng kết nối Control Database
    public static void closeConnection() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            // Ghi thông báo lỗi mà không in ra stack trace
            System.err.println("Lỗi khi đóng kết nối cơ sở dữ liệu: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        // Phương thức main có thể để trống hoặc bạn có thể thêm các trường hợp thử nghiệm ở đây
    }
}
