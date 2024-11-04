package Dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import SetUp.HostingerEmail;

public class datamark_db {
	private static final String URL = "jdbc:mysql://localhost:3306/datamark_db";
	private static final String USER = "root";
	private static final String PASSWORD = "123456";
	private static final String email = "20130374@st.hcmuaf.edu.vn";
	private static Connection connection = null;

	// Khởi tạo control_db để quản lý logging và lấy fileId mới nhất
	static control_db ctdb = new control_db();
	private static final staging_db stdb = new staging_db();
	private static final DWdienmayxanh dwdmx = new DWdienmayxanh();
	static Integer latestFileId = ctdb.getLatestFileId();

	// Phương thức lấy kết nối đến database
	public static Connection getConnection() throws ClassNotFoundException, SQLException {
		if (connection == null || connection.isClosed()) {
			try {
				Class.forName("com.mysql.cj.jdbc.Driver"); // Nạp driver MySQL
				connection = DriverManager.getConnection(URL, USER, PASSWORD);
			} catch (SQLException e) {
				e.printStackTrace();
				// 17.2 Gửi email thông báo lỗi
				HostingerEmail.sendEmail(email, "Kết nối Data Mark", "Kết nối thất bại");
				// đóng kết nối
				dwdmx.closeConnection();
				stdb.closeConnection();
				ctdb.closeConnection();
				throw e;
			}
		}
		return connection;
	}

	// 17.1 Tải dữ liệu từ DW vào Datamark
	// Phương thức thực hiện tải dữ liệu vào Data Mart
	public void loadDataToDataMart() throws ClassNotFoundException {
		String loadDataSql = """
				INSERT INTO datamark_db.DataMart (productName, description, category, brand, size, originalPrice, discountedPrice, date)
				SELECT
				    pd.productName,
				    pd.description,
				    cd.categoryName,
				    bd.brandName,
				    pd.size,
				    pd.originalPrice,
				    pd.discountedPrice,
				    dt.date
				FROM DW_dienmayxanh.Product_fact AS pf
				JOIN DW_dienmayxanh.Product_dim AS pd ON pf.productID = pd.productID
				JOIN DW_dienmayxanh.Category_dim AS cd ON pd.categoryID = cd.categoryID
				JOIN DW_dienmayxanh.Brand_dim AS bd ON pd.brandID = bd.brandID
				JOIN DW_dienmayxanh.Date_dim AS dt ON pf.dateID = dt.dateID;
				""";

		try (Connection conn = DWdienmayxanh.getConnection(); Statement stmt = conn.createStatement()) {
			// Thực thi câu lệnh INSERT để tải dữ liệu
			stmt.execute(loadDataSql);
			System.out.println("Dữ liệu đã được tải vào Data Mart thành công.");

			// 18. Cập nhật trạng thái trong logs "Success Loading"
			ctdb.logToDatabase(latestFileId, "Load", "Success Loading", "Dữ liệu đã được tải lên Data Mart");
		} catch (SQLException e) {
			System.out.println("Lỗi khi tải dữ liệu vào Data Mart: " + e.getMessage());
			// Ghi trạng thái lỗi vào bảng logs nếu tải thất bại
			ctdb.logToDatabase(latestFileId, "Load", "Failed", "Tải dữ liệu lên Data Mart thất bại");
			e.printStackTrace();
		}
	}

	// 19. Hiên thị dữ liệu lên UI
	public void displayDataMart() throws ClassNotFoundException {
	    String query = "SELECT * FROM datamark_db.DataMart";
	    try (Connection conn = getConnection();
	         Statement stmt = conn.createStatement();
	         ResultSet rs = stmt.executeQuery(query)) {

	        // Sử dụng StringBuilder để lưu trữ danh sách kết quả
	        StringBuilder result = new StringBuilder();
	        result.append("Danh sách dữ liệu trong Data Mart:\n");

	        while (rs.next()) {
	            String productName = rs.getString("productName");
	            String description = rs.getString("description");
	            String category = rs.getString("category");
	            String brand = rs.getString("brand");
	            String size = rs.getString("size");
	            double originalPrice = rs.getDouble("originalPrice");
	            double discountedPrice = rs.getDouble("discountedPrice");
	            String date = rs.getString("date");

	            // Tạo chuỗi cho từng sản phẩm
	            String productInfo = String.format(
	                    "Tên sản phẩm: %s, Mô tả: %s, Danh mục: %s, Thương hiệu: %s, Kích thước: %s, Giá gốc: %.2f, Giá khuyến mãi: %.2f, Ngày: %s",
	                    productName, description, category, brand, size, originalPrice, discountedPrice, date);

	            // Thêm thông tin sản phẩm vào StringBuilder
	            result.append(productInfo).append("\n");
	        }

	        // In ra danh sách sản phẩm
	        System.out.println(result.toString());

	        // Ghi log trạng thái hoàn tất
	        ctdb.logToDatabase(latestFileId, "FINSH", "FINSH", "HOÀN TẤT");

	    } catch (SQLException e) {
	        System.out.println("Lỗi khi truy vấn dữ liệu từ Data Mart: " + e.getMessage());
	        e.printStackTrace();
	    } finally {
	        // Đảm bảo đóng kết nối
	        closeConnection();
	        dwdmx.closeConnection();
	        stdb.closeConnection();
	        ctdb.closeConnection();
	    }
	}

	// 21. Đóng kết nối db Data Mart
	public static void closeConnection() {
		try {
			if (connection != null && !connection.isClosed()) {
				connection.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws ClassNotFoundException {
	
	}
}
