package Dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import SetUp.HostingerEmail;

public class DWdienmayxanh {
	private static final String URL = "jdbc:mysql://localhost:3306/dw_dienmayxanh";
	private static final String USER = "root";
	private static final String PASSWORD = "123456";
	private static final String email = "20130374@st.hcmuaf.edu.vn";
	private static Connection connection = null;

	private static final control_db ctdb = new control_db();
	private static final staging_db stdb = new staging_db();
	private static final Integer latestFileId = ctdb.getLatestFileId();

	// Phương thức lấy kết nối đến database
	public static Connection getConnection() throws ClassNotFoundException, SQLException {
		if (connection == null || connection.isClosed()) {
			try {
				Class.forName("com.mysql.cj.jdbc.Driver");
				connection = DriverManager.getConnection(URL, USER, PASSWORD);
			} catch (SQLException e) {
				//13.2 Gửi email thông báo lỗi
				HostingerEmail.sendEmail(email, "Kết nối Datawarehouse Database", "Kết nối thất bại");
				//13.3 Đóng kết nối database db staging
				stdb.closeConnection();
				throw e;
			}
		}
		return connection;
	}

	// Phương thức thực hiện chuyển đổi dữ liệu
	public void transformData() throws ClassNotFoundException {
		try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
			//14. Chuyển đổi dữ liệu theo bảng dim
			stmt.execute(getTransformDateDimSQL());
			stmt.execute(getTransformBrandDimSQL());
			stmt.execute(getTransformCategoryDimSQL());
			stmt.execute(getTransformProductDimSQL());
			stmt.execute(getTransformProductFactSQL());
			//15. Cập nhật trạng thái trong logs "Success Transfroming"
			ctdb.logToDatabase(latestFileId, "Transform", "Success Transforming",
					"Dữ liệu đã được chuyển đổi thành công");
			System.out.println("Quá trình chuyển đổi dữ liệu hoàn tất.");
		} catch (SQLException e) {
			ctdb.logToDatabase(latestFileId, "Transform", "Failed", "Chuyển đổi dữ liệu thất bại");
			e.printStackTrace();
		}
	}

	// Các câu lệnh SQL chuyển đổi (lần lượt cho từng bảng)
	private String getTransformDateDimSQL() {
		return """
				INSERT INTO DW_dienmayxanh.Date_dim (date, day, month, year, dayOfWeek)
				SELECT DISTINCT
					pd.date, DAY(pd.date), MONTH(pd.date), YEAR(pd.date), DAYNAME(pd.date)
				FROM staging_db.product_daily AS pd
				WHERE pd.date IS NOT NULL
				ON DUPLICATE KEY UPDATE
					day = DAY(pd.date),
					month = MONTH(pd.date),
					year = YEAR(pd.date),
					dayOfWeek = DAYNAME(pd.date);
				""";
	}

	private String getTransformBrandDimSQL() {
		return """
				INSERT INTO DW_dienmayxanh.Brand_dim (brandName)
				SELECT DISTINCT pd.brand FROM staging_db.product_daily AS pd
				WHERE pd.brand IS NOT NULL
				ON DUPLICATE KEY UPDATE brandName = pd.brand;
				""";
	}

	private String getTransformCategoryDimSQL() {
		return """
				INSERT INTO DW_dienmayxanh.Category_dim (categoryName)
				SELECT DISTINCT pd.category FROM staging_db.product_daily AS pd
				WHERE pd.category IS NOT NULL
				ON DUPLICATE KEY UPDATE categoryName = pd.category;
				""";
	}

	private String getTransformProductDimSQL() {
		return """
				INSERT INTO DW_dienmayxanh.Product_dim (productName, description, categoryID, brandID, size, originalPrice, discountedPrice)
				SELECT
					pd.productName, pd.description,
					(SELECT cd.categoryID FROM DW_dienmayxanh.Category_dim AS cd WHERE cd.categoryName = pd.category LIMIT 1),
					(SELECT bd.brandID FROM DW_dienmayxanh.Brand_dim AS bd WHERE bd.brandName = pd.brand LIMIT 1),
					pd.size, pd.originalPrice, pd.discountedPrice
				FROM staging_db.product_daily AS pd;
				""";
	}

	private String getTransformProductFactSQL() {
		return """
				INSERT INTO DW_dienmayxanh.Product_fact (productID, dateID, discount, rating)
				SELECT
					(SELECT productID FROM DW_dienmayxanh.Product_dim WHERE productName = pd.productName LIMIT 1),
					(SELECT dateID FROM DW_dienmayxanh.Date_dim WHERE date = pd.date LIMIT 1),
					pd.discount, pd.rating
				FROM staging_db.product_daily AS pd;
				""";
	}
    //22. Đóng kết nối DataWH
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
