package SetUp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import Dao.control_db;

public class TxtToCsvConverter {
	static control_db ctdb = new control_db();
	// Lấy ID mới nhất từ file_config
	static Integer latestFileId = ctdb.getLatestFileId();

	public static void convertTxtToCsv(String txtFilePath, String csvFilePath) {
		File csvFile = new File(csvFilePath);

		// Kiểm tra và xóa file cũ nếu đã tồn tại.
		if (csvFile.exists() && !csvFile.delete()) {
			System.out.println("Không thể xóa file cũ: " + csvFilePath);
			return;
		}

		try (
				// Đọc file TXT với mã hóa UTF-8
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(new FileInputStream(txtFilePath), StandardCharsets.UTF_8));
				// Ghi file CSV với mã hóa UTF-8 và thêm BOM
				BufferedWriter writer = new BufferedWriter(
						new OutputStreamWriter(new FileOutputStream(csvFilePath), StandardCharsets.UTF_8))) {
			// Thêm BOM vào đầu file để Excel nhận diện UTF-8
			writer.write("\uFEFF");

			// Ghi tiêu đề vào CSV
			writer.write(
					"productID,productName,description,category,originalPrice,discountedPrice,discount,brand,size,rating,date");
			writer.newLine();

			String line;
			boolean isFirstLine = true;
			while ((line = reader.readLine()) != null) {
				line = line.trim();

				// Kiểm tra nếu dòng không rỗng
				if (!line.isEmpty()) {
					if (isFirstLine) {
						// Bỏ qua dòng tiêu đề đầu tiên trong file txt
						isFirstLine = false;
						continue;
					}

					writer.write(line);
					writer.newLine();
				}
			}

			System.out.println("Chuyển đổi thành công từ .txt sang .csv");

		} catch (IOException e) {
			System.out.println("Chuyển đổi thành công từ .txt sang .csv thất bại");
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		String txtFilePath = "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\products.txt"; // Đường dẫn tới file
																								// .txt
		String csvFilePath = "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\products.csv"; // Đường dẫn tới file
																								// .csv đầu ra
		convertTxtToCsv(txtFilePath, csvFilePath);
	}
}
