package SetUp;

import java.util.Date;
import java.util.Properties;
import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

public class HostingerEmail {
	static final String from = "heogaming@heogaming.com";
	static final String password = "Nhom5DW@";

	public static boolean sendEmail(String to, String tieuDe, String noiDung) {
		// Properties : khai báo các thuộc tính
		Properties props = new Properties();
		props.put("mail.smtp.host", "smtp.hostinger.com"); // SMTP HOST
		props.put("mail.smtp.port", "465"); // SSL port
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.socketFactory.port", "465");
		props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");

		// create Authenticator
		Authenticator auth = new Authenticator() {
			@Override
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(from, password);
			}
		};

		// Phiên làm việc
		Session session = Session.getInstance(props, auth);

		// Tạo một tin nhắn
		MimeMessage msg = new MimeMessage(session);

		try {
			// Kiểu nội dung
			msg.addHeader("Content-type", "text/plain; charset=UTF-8");

			// Người gửi
			msg.setFrom(from);

			// Người nhận
			msg.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to, false));

			// Tiêu đề email
			msg.setSubject(tieuDe);

			// Quy định ngày gửi
			msg.setSentDate(new Date());

			// Nội dung dạng văn bản thuần
			msg.setText(noiDung, "UTF-8");

			// Gửi email
			Transport.send(msg);
			System.out.println("Gửi email thành công");
			return true;
		} catch (Exception e) {
			System.out.println("Gặp lỗi trong quá trình gửi email");
			e.printStackTrace();
			return false;
		}
	}

	public static void main(String[] args) {
        // Test sending email
        String recipient = "20130374@st.hcmuaf.edu.vn"; // Change this to the recipient's email
        String subject = "Test Email from Hostinger"; // Email subject
        String body = "This is a test email sent from the HostingerEmail class."; // Email body

        boolean result = sendEmail(recipient, subject, body);
        if (result) {
            System.out.println("Email đã được gửi thành công!");
        } else {
            System.out.println("Gửi email thất bại.");
        }
    }
}
