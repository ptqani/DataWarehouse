package SetUp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import Dao.control_db;

public class TxtToCsvConverter {
	static control_db ctdb = new control_db();
	// Lấy ID mới nhất từ file_config
	static Integer latestFileId = ctdb.getLatestFileId();

	public static void convertTxtToCsv(String txtFilePath, String csvFilePath) {
		File csvFile = new File(csvFilePath);

		// Kiểm tra và xóa file cũ nếu đã tồn tại
		if (csvFile.exists()) {
			if (csvFile.delete()) {
				System.out.println("File cũ đã bị xóa: " + csvFilePath);
			} else {
				System.out.println("Không thể xóa file cũ: " + csvFilePath);
				return; // Dừng quá trình nếu không thể xóa
			}
		}

		try (BufferedReader reader = new BufferedReader(new FileReader(txtFilePath));
				BufferedWriter writer = new BufferedWriter(new FileWriter(csvFilePath))) {

			// Ghi tiêu đề vào CSV
			writer.write(
					"productID,productName,description,category,originalPrice,discountedPrice,discount,brand,size,rating,date");
			writer.newLine();

			String line;
			while ((line = reader.readLine()) != null) {
				line = line.trim();

				// Kiểm tra nếu dòng không rỗng
				if (!line.isEmpty()) {
					// Ghi trực tiếp dòng vào file CSV
					writer.write(line);
					writer.newLine();
				}
			}

			System.out.println("Chuyển đổi thành công từ .txt sang .csv");
			//6. Cập nhật trạng thái trong logs "Success"
			ctdb.logToDatabase(latestFileId, "Save", "Success", "Lưu dữ liệu vào file products.csv thành công");
		} catch (IOException e) {
			//5.2 Ghi vào bảng logs với trạng thái "Failed"
			// Ghi log với ID mới nhất
			ctdb.logToDatabase(latestFileId, "Save", "Failed", "Lưu dữ liệu vào file products.csv thất bại");
			ctdb.closeConnection();
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		String txtFilePath = "D:\\DW\\text1.txt"; // Đường dẫn tới file .txt
		String csvFilePath = "D:\\DW\\text2.csv"; // Đường dẫn tới file .csv đầu ra
		convertTxtToCsv(txtFilePath, csvFilePath);
	}
}
