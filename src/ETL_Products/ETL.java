package ETL_Products;

import java.sql.Connection;
import java.sql.SQLException;

import Dao.DWdienmayxanh;
import Dao.control_db;
import Dao.datamark_db;
import Dao.staging_db;
import SetUp.HostingerEmail;
import SetUp.TxtToCsvConverter;

public class ETL {
    private static final String email = "20130374@st.hcmuaf.edu.vn";
    static Integer latestFileId;

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        // Extracting
        // 1. Lấy dữ liệu lúc 9 giờ sáng hàng ngày
        // 2. Kết nối Control Database
        control_db ctdb = new control_db();
        System.out.println("Kết nối Control Database...");
        //3. Kết nối
        Connection conn = ctdb.getConnection(); // Get connection without try-catch block
        if (conn == null) {
            System.out.println("Kết nối Control Database thất bại");
            //3.2 Gửi email thông báo lỗi
            HostingerEmail.sendEmail(email, "Kết nối Control Database", "Kết nối thất bại");
            //3.3 Ghi vào bảng logs với trạng thái "Failed"
            ctdb.logToDatabase(latestFileId, "Connect", "Failed", "Kết nối với csdl thất bại");
            return; 
        }
        System.out.println("Kết nối Control Database thành công");
        //3.1 Xác Minh path Mới nhất trong Control Database (file_config)
        latestFileId = ctdb.getLatestFileId(); // Get latest file ID from control database
        System.out.println(latestFileId);

        // 4. Trích xuất dữ liệu
        TxtToCsvConverter terconvertTxtToCsv = new TxtToCsvConverter();
        // 5. Trích xuất
        try {
            System.out.println("Đang lưu dữ liệu vào file products.csv...");
            // 5.1 Lưu dữ liệu vào products.csv
            terconvertTxtToCsv.convertTxtToCsv("C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\products.txt",
                    "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\products.csv");
            System.out.println("Lưu dữ liệu vào file products.csv thành công");
            // 6. Cập nhật trạng thái trong logs "Success"
            ctdb.logToDatabase(latestFileId, "Save", "Success", "Lưu dữ liệu vào file products.csv thành công");
        } catch (Exception e) {
            System.out.println("Lưu dữ liệu vào file products.csv thất bại");
            // 5.2 Ghi vào bảng logs với trạng thái "Failed"
            ctdb.logToDatabase(latestFileId, "Save", "Failed", "Lưu dữ liệu vào file products.csv thất bại");
            return; // Exit the process if file conversion fails
        }

        // Transform
        // 7. Kết nối database db staging
        staging_db stdb = new staging_db();
        System.out.println("Kết nối database staging...");
        //8. Kết nối
        conn = stdb.getConnection(); 
        if (conn == null) {
            System.out.println("Kết nối database staging thất bại");
         //8.2 Gửi email thông báo lỗi
            HostingerEmail.sendEmail(email, "Kết nối Staging Database", "Kết nối thất bại");
         //8.3 Ghi vào bảng logs với trạng thái "Failed"
            ctdb.logToDatabase(latestFileId, "Connect", "Failed", "Kết nối với csdl Staging thất bại");
            return; 
        }
        System.out.println("Kết nối database staging thành công");
        //8.1. Truy cập vào file_path của file_config 
        System.out.println( ctdb.getLatestFilePath());

        // 9. Đọc file CSV và 10. Lưu dữ liệu
        try {
            System.out.println("Đang tải dữ liệu vào bảng tạm temp_product_daily...");
            // 10.1 Tải dữ liệu vào Staging bảng tạm temp_product_daily
            
            stdb.loadCsvToTempTable();
            System.out.println("Tải dữ liệu vào bảng tạm thành công");
        } catch (Exception e) {
            System.out.println("Tải dữ liệu vào bảng tạm thất bại");
            // 10.2 Ghi vào bảng logs với trạng thái "Failed"
            ctdb.logToDatabase(latestFileId, "Load", "Failed", "Tải dữ liệu vào bảng tạm thất bại");
            return;
        }

        // 11. Làm sạch dữ liệu và lưu vào bảng chính product_daily
        System.out.println("Làm sạch dữ liệu...");
        stdb.updateDataInTempTable();
        stdb.insertDataFromTempToMain();
        System.out.println("Dữ liệu đã được làm sạch và lưu vào bảng chính");

        // 12. Cập nhật trạng thái trong logs "Success Extracting"
        ctdb.logToDatabase(latestFileId, "Extract", "Success", "Tải dữ liệu vào bảng chính thành công");

        // 13. Kết nối database DW.dienmayxanh
        DWdienmayxanh dwdmx = new DWdienmayxanh();
        System.out.println("Kết nối database DW.dienmayxanh...");
        //14. Kết nối
        conn = dwdmx.getConnection(); 
        if (conn == null) {
            System.out.println("Kết nối database DW.dienmayxanh thất bại");
        //14.2 Gửi email thông báo lỗi
            HostingerEmail.sendEmail(email, "Kết nối db DWdienmayxanh", "Kết nối thất bại");
        //14.3 Ghi vào bảng logs với trạng thái "Failed"
            ctdb.logToDatabase(latestFileId, "Connect", "Failed", "Kết nối với csdl DWdienmayxanh thất bại");
            return; 
        }
        System.out.println("Kết nối database DW.dienmayxanh thành công");

        // 14.1 Chuyển đổi dữ liệu từ Staging Product_daily sang Product_fact, Product_dim, Date_dim, Brand_dim, Category_dim
        // 15. Chuyển đổi dữ liệu theo bảng dim
        System.out.println("Đang chuyển đổi dữ liệu...");
        dwdmx.transformData();
        System.out.println("Chuyển đổi dữ liệu thành công");

        // 16. Cập nhật trạng thái trong logs "Success Transforming"
        ctdb.logToDatabase(latestFileId, "Transform", "Success", "Dữ liệu đã được chuyển đổi thành công");

        // Load
        // 17. Kết nối db Data Mart
        datamark_db dtmdb = new datamark_db();
        System.out.println("Kết nối database Data Mart...");
        //18. Kết nối
        conn = dtmdb.getConnection(); 
        if (conn == null) {
            System.out.println("Kết nối database Data Mart thất bại");
        //18.2 Gửi email thông báo lỗi
            HostingerEmail.sendEmail(email, "Kết nối db datamark_db", "Kết nối thất bại");
        //18.3 Ghi vào bảng logs với trạng thái "Failed"
            ctdb.logToDatabase(latestFileId, "Connect", "Failed", "Kết nối với csdl datamark_db thất bại");
            return; // Exit the process if connection fails
        }
        System.out.println("Kết nối database Data Mart thành công");

        // 18.1 Tải dữ liệu từ DW vào Data Mart
        System.out.println("Đang tải dữ liệu vào Data Mart...");
        dtmdb.loadDataToDataMart();
        System.out.println("Tải dữ liệu vào Data Mart thành công");

        // 19. Cập nhật trạng thái trong logs "Success Loading"
        ctdb.logToDatabase(latestFileId, "Load", "Success", "Dữ liệu đã được tải lên Data Mart");

        // 20. Hiển thị dữ liệu lên UI
        System.out.println("Hiển thị dữ liệu lên UI...");
        dtmdb.displayDataMart();
        System.out.println("Hiển thị dữ liệu thành công");

        // 21. Cập nhật trạng thái trong logs "Finish"
        ctdb.logToDatabase(latestFileId, "Finish", "Success", "Quá trình ETL hoàn tất");

        // 22. Đóng kết nối db Data Mart
        System.out.println("Đóng kết nối database Data Mart...");
        dtmdb.closeConnection();
        // 23. Đóng kết nối db DW
        System.out.println("Đóng kết nối database DW...");
        dwdmx.closeConnection();
        // 24. Đóng kết nối database db staging
        System.out.println("Đóng kết nối database Staging...");
        stdb.closeConnection();
        // 25. Đóng kết nối Control Database
        System.out.println("Đóng kết nối Control Database...");
        ctdb.closeConnection();
        // Đóng kết nối
        System.out.println("Đóng tất cả các kết nối thành công");
    }
}
