package sparkTesting;

import java.io.IOException;
import java.net.*;
import static spark.Spark.*;

// https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/net/ServerSocket.html
public class WebInterface implements Runnable {

	private int port = 80;
	private ServerSocket server;

	public WebInterface() throws IOException {
		server = new ServerSocket(port);
	}

	@Override
	public void run() {
		serverTest();
	}

	//localhost:4567 
	/*	http://sparkjava.com/documentation
	 *  https://static.javadoc.io/com.sparkjava/spark-core/2.7.2/spark/Spark.html */
	private void serverTest() {
		get("/spark_java", (req, res) -> "aaaa" + req.url());
		//TODO get string from react front end (with post ?)
	}


	private void serversocketTest() {
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

