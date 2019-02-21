package sparkTesting;

import java.io.IOException;
import java.net.*;

// https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/net/ServerSocket.html
// https://docs.oracle.com/en/java/javase/11/docs/api/jdk.httpserver/com/sun/net/httpserver/HttpServer.html
public class WebInterface implements Runnable {

	private int port = 80;
	private ServerSocket server;

	public WebInterface() throws IOException {
		server = new ServerSocket(port);
	}

	@Override
	public void run() {
		while (true) {
			try {
				Socket client = server.accept();
				//TODO POST request, parse result...	

			} catch (SocketTimeoutException s) {
				System.out.println("Socket timed out!");
				break;
			} catch (IOException e) {
				e.printStackTrace();
				break;
			}
		}
	}
}
