package transform;

import java.math.BigDecimal;

public class DataProcessor {
	// Phương thức thay thế giá trị null hoặc rỗng bằng "NA"
	public static String ensureValue(String value) {
		return (value == null || value.trim().isEmpty()) ? "NA" : value.trim();
	}

	public static String cleanPrice(String price) {
		// Xóa ký tự không phải số, nếu rỗng thay bằng "NA"
		String cleanPrice = price.replaceAll("[^\\d]", "");
		return ensureValue(cleanPrice);
	}

	public static String cleanText(String text) {
		// Loại bỏ khoảng trắng dư thừa và đảm bảo không để trống
		return ensureValue(text);
	}

	public static double calculateDiscountPercentage(String originalPrice, String discountedPrice) {
		// Chuyển đổi giá từ chuỗi sang số
		try {
			double original = Double.parseDouble(cleanPrice(originalPrice));
			double discounted = Double.parseDouble(cleanPrice(discountedPrice));
			if (original > 0 && discounted > 0) {
				return ((original - discounted) / original) * 100;
			}
		} catch (NumberFormatException e) {
			e.printStackTrace();
		}
		return 0.0; // Nếu không tính được, trả về 0
	}

	public static BigDecimal parsePrice(String price) {
		if (price == null || price.trim().isEmpty()) {
			return BigDecimal.ZERO; // Nếu giá trống, trả về 0
		}

		// Loại bỏ tất cả ký tự không phải số và dấu phân cách thập phân (.,)
		price = price.replaceAll("[^0-9.,]", "").replace(",", ".");

		// Kiểm tra giá trị có hợp lệ sau khi loại bỏ ký tự không hợp lệ
		if (price.isEmpty()) {
			return BigDecimal.ZERO;
		}

		try {
			return new BigDecimal(price); // Chuyển đổi thành BigDecimal
		} catch (NumberFormatException e) {
			System.err.println("Lỗi khi chuyển đổi giá: " + price); // In ra giá trị không hợp lệ để debug
			return BigDecimal.ZERO; // Trả về giá trị mặc định khi có lỗi
		}
	}

}
