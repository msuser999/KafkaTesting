package kafkaApp;

import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import javax.swing.text.DefaultCaret;

import org.apache.kafka.clients.producer.*;
import java.util.Properties;

public class GUI {

	private JTextArea console; // Console in the consumer window
	private String serverName = "localhost";
	private int topicPort = 2181; // port used for topics
	private int messagePort = 9092; // port used for the consumer and producer
	private String currentTopic;
	private KafkaProducer<String, String> producer = createProducer();

	public void createGUI() {
		JFrame frame = new JFrame("Kafka App");
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		Container pane = frame.getContentPane();
		pane.setLayout(new BoxLayout(pane, BoxLayout.Y_AXIS));

		// Other panels
		pane.add(connectionPanel());
		pane.add(consumerPanel());
		pane.add(producerPanel());

		frame.pack();
		frame.setVisible(true);
		frame.addWindowListener(new WindowAdapter() {
			public void windowClosing(WindowEvent we) {
				//consumer.close();// close the consumer and producer on exit (necessary??), commented out bcs closed in ConsumerThread's run()
				producer.close();
				System.exit(0);
			}
		});
	}

	private JPanel connectionPanel() {
		// Init.
		JPanel panel = new JPanel();
		panel.setLayout(new BoxLayout(panel, BoxLayout.X_AXIS));

		// Components
		// TODO server connection check, topic list and selection ...?
		currentTopic = "mytopic";
		JLabel currTopicLbl = new JLabel("Current topic name - " + currentTopic);

		panel.add(currTopicLbl);
		return panel;
	}

	private JPanel consumerPanel() {
		// Panel init.
		JPanel panel = new JPanel();
		panel.setLayout(new BoxLayout(panel, BoxLayout.X_AXIS));
		panel.setBorder(BorderFactory.createTitledBorder("Consumer"));
		JPanel btnPanel = new JPanel();
		btnPanel.setLayout(new BoxLayout(btnPanel, BoxLayout.Y_AXIS));

		// Components
		console = new JTextArea(); // Console for consumer messages
		console.setEditable(false);
		DefaultCaret dc = (DefaultCaret) console.getCaret(); // make it so console is always scrolled to the bottom line
		dc.setUpdatePolicy(DefaultCaret.ALWAYS_UPDATE);
		JScrollPane scroll = new JScrollPane(console); // add scrolling to textarea
		scroll.setPreferredSize(new Dimension(400, 100)); // size of console itself

		JButton updateBtn = new JButton("Start consumer"); // Button for getting messages from the consumer
		updateBtn.setAlignmentX(Component.CENTER_ALIGNMENT);
		updateBtn.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent ae) {
				console.setText("");
				updateBtn.setEnabled(false); //TODO add button for closing consumer, swap places with /updateBtn/ here
				Thread t = new Thread(new ConsumerThread(serverName, messagePort, currentTopic, console));
				t.start();
			}
		});

		JButton clearBtn = new JButton("Clear console"); // Button for clearing the console
		clearBtn.setAlignmentX(Component.CENTER_ALIGNMENT);
		clearBtn.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent ae) {
				console.setText("");
			}
		});

		// Add components to panels
		panel.add(scroll);
		btnPanel.add(updateBtn);
		btnPanel.add(clearBtn);
		panel.add(btnPanel);
		return panel;
	}


	private JPanel producerPanel() {
		// Panel init.
		JPanel panel = new JPanel();
		panel.setLayout(new BoxLayout(panel, BoxLayout.X_AXIS));
		panel.setBorder(BorderFactory.createTitledBorder("Producer"));

		// Components
		JTextField field = new JTextField(); // Field for messages

		JButton sendBtn = new JButton("Send message"); // Button for sending messages
		sendBtn.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent ae) {
				producerSendMessage(field);
			}
		});

		// Add components to panel
		panel.add(field);
		panel.add(sendBtn);
		return panel;
	}

	private void producerSendMessage(JTextField field) {
		this.producer.send(new ProducerRecord<String, String>(currentTopic, field.getText() + " @ " + java.time.Instant.now()));
		field.setText("");
	}

	// mby best to add producer to own class too
	private KafkaProducer<String, String> createProducer() {
		Properties props = new Properties();
		props.put("bootstrap.servers", serverName + ":" + messagePort);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return new KafkaProducer<>(props);
	}
}
