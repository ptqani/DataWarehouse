package ETL_Products;

import java.sql.SQLException;

import Dao.DWdienmayxanh;
import Dao.control_db;
import Dao.datamark_db;
import Dao.staging_db;
import SetUp.TxtToCsvConverter;

public class ETL {
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		// Extracting
		// 1. Lấy dữ liệu lúc 9 giờ sáng hàng ngày
		// 2. Kết nối Control Database
		control_db ctdb = new control_db();
		// 3. Kết nối
		ctdb.getConnection();
		// 4. Trích xuất dữ liệu
		// 5. Trích xuất
		TxtToCsvConverter terconvertTxtToCsv = new TxtToCsvConverter();
		// 5.1 Lưu dữ liệu vào products.csv
		terconvertTxtToCsv.convertTxtToCsv("C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\products.txt",
				"C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\products.csv");
		// Transfrom
		// 7. Kết nối database db staging
		staging_db stdb = new staging_db();
		// 8. Kết nối
		stdb.getConnection();
		// 9. Đọc file CSV
		// 10. Lưu dữ liệu
		stdb.loadCsvToTempTable();
		stdb.insertDataFromTempToMain();
		// 12. Kết nối database DW.dienmayxanh
		DWdienmayxanh dwdmx = new DWdienmayxanh();
		// 13. Kết nối
		dwdmx.getConnection();
		// 13.1 Chuyển đổi dữ liệu từ Staging Product_daily sang
		// Product_fact,Product_dim,Date_dim,Brand_dim,Category_dim
		dwdmx.transformData();
		// Load
		// 16. Kết nối db Data Mart
		datamark_db dtmdb = new datamark_db();
		// 17. Kết nối
		dtmdb.getConnection();
		// 17.1 Tải dữ liệu từ DW vào Datamark
		dtmdb.loadDataToDataMart();
		// 19. Hiên thị dữ liệu lên UI
		dtmdb.displayDataMart();

	}
}
