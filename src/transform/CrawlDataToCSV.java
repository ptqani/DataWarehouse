package transform;

import java.io.BufferedReader;
import java.io.FileReader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;

public class CrawlDataToCSV {
	private static final String URL = "jdbc:mysql://localhost:3306/staging_db?useSSL=false";
	private static final String USER = "root";
	private static final String PASSWORD = "Chihai000@";

	public static void main(String[] args) {
		String url = "https://www.dienmayxanh.com/flashsale"; // URL trang cần crawl
		String csvFilePath = "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\products.csv";

//		try (FileWriter csvWriter = new FileWriter(csvFilePath)) {
//			csvWriter.append(
//					"productID,productName,category,originalPrice,discountedPrice,discount,brand,size,rating,date\n");
//
//			Document document = Jsoup.connect(url).timeout(60000) // Timeout 60 giây
//					.get();
//			// Lấy danh sách sản phẩm
//			Elements products = document.select(".item"); // Cần kiểm tra selector từ HTML
//
//			for (Element product : products) {
//				String productID = DataProcessor.ensureValue(product.attr("data-id"));
//				String productName = DataProcessor.ensureValue(product.select("a").attr("data-name"));
//				String category = DataProcessor.ensureValue(product.select("a").attr("data-cate"));
//				String originalPrice = DataProcessor.ensureValue(product.select(".price-old.black").text());
//				String discountedPrice = DataProcessor.ensureValue(product.select(".price").text());
//				String discount = DataProcessor.ensureValue(product.select(".percent").text());
//				String brand = DataProcessor.ensureValue(product.select("a").attr("data-brand"));
//				String size = DataProcessor.ensureValue(product.select(".item-compare span").text());
//				String rating = DataProcessor.ensureValue(product.select(".vote-txt b").text());
//				String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
//
//				originalPrice = DataProcessor.cleanPrice(originalPrice);
//				discountedPrice = DataProcessor.cleanPrice(discountedPrice);
//
//				csvWriter.append(productID).append(",").append(productName).append(",").append(category).append(",")
//						.append(originalPrice).append(",").append(discountedPrice).append(",").append(discount)
//						.append(",").append(brand).append(",").append(size).append(",").append(rating).append(",")
//						.append(date).append("\n");
//			}
		// Kết nối MySQL
		try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
				PreparedStatement stmt = conn.prepareStatement(
						"INSERT INTO staging_products (productID, productName, category, originalPrice, discountedPrice, discount, brand, size, rating, date) "
								+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
			// Đọc từ CSV và chèn vào DB
			BufferedReader csvReader = new BufferedReader(new FileReader(csvFilePath));
			String row;
			while ((row = csvReader.readLine()) != null) {
				String[] data = row.split(","); // Tách dữ liệu
				stmt.setString(1, data[0]); // productID
				stmt.setString(2, data[1]); // productName
				stmt.setString(3, data[2]); // category

				// Làm sạch và kiểm tra giá trị trước khi chuyển đổi
				BigDecimal originalPriceValue = DataProcessor.parsePrice(data[3]);
				BigDecimal discountedPriceValue = DataProcessor.parsePrice(data[4]);

				stmt.setBigDecimal(4, originalPriceValue); // originalPrice
				stmt.setBigDecimal(5, discountedPriceValue); // discountedPrice

				stmt.setString(6, data[5]); // discount
				stmt.setString(7, data[6]); // brand
				stmt.setString(8, data[7]); // size
				stmt.setString(9, data[8]); // rating
				stmt.setTimestamp(10, Timestamp.valueOf(data[9])); // date
				stmt.addBatch();
			}
			stmt.executeBatch();
			System.out.println("Dữ liệu đã được chèn vào bảng tạm.");
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Dữ liệu đã được lưu vào file CSV: " + csvFilePath);
//		} catch (IOException e) {
//			System.err.println("Có lỗi xảy ra khi crawl dữ liệu: " + e.getMessage());
//		}
	}

}