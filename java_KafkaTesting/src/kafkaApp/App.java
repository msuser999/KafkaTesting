package kafkaApp;

import javax.swing.*;

public class App {

	public static void main(String[] args) {
		SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				GUI gui = new GUI();
				gui.createGUI();
			}
		});
	}


}
