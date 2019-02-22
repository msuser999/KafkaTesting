const fs      = require("fs");
const request = require("request");

const kafka_api_url = "https://cankafka.northeurope.cloudapp.azure.com"
//base64 form for http authentication header
const auth_acc = "nginx";
const auth_pw  = "iHrpmWQ4hj2BSxACfsP2V5Uk";
var auth_header = Buffer.from(auth_acc + ":" + auth_pw).toString("base64");
auth_header     = "Basic " + auth_header;

const body_base = {
    url: kafka_api_url,
    headers: {
	Authorization: auth_header
    }
};

const kafka = require("kafka-node");
const client = new kafka.KafkaClient({
    kafkaHost: "cankafka.northeurope.cloudapp.azure.com:9092",
    sslOptions: {
	ca: [fs.readFileSync("tls/ca_cert.pem")],
	key: fs.readFileSync("tls/js_key.pem"),
	cert: fs.readFileSync("tls/js_cert.pem"),
        passphrase: "j!z,KS)u]g656aHY]t5P"
    }
});
const admin = new kafka.Admin(client);

const def_cb = () => {};


exports.create_topic = (topic) => {
    client.createTopics([topic], (err, res) => console.log(err));
};

exports.list_topics = (cb = def_cb) => {
    admin.listTopics( (err, res) => cb(Object.keys( res[1]['metadata'] )) );
};

exports.list_connectors = (cb = def_cb) => {
    var body = Object.create(body_base);
    body.url += "/connectors";
    body.method = "GET";

    request(body, (err, resp, body) => cb(err, resp, body));
};

exports.update_connector = (config, cb = def_cb) =>  {
    var body = Object.create(body_base);
    body.url += "/connectors/" + config.name + "/config";
    body.method = "PUT";
    body.headers["Content-type"] = "application/json";
    //need to flatten the json
    body.json = {...config, ...config.config};
    delete body.json.config;

    console.log(body.url);
    request(body, (err, resp, body) => cb(err, resp, body));
};

exports.delete_connector = (name, cb = def_cb) => {
    var body = Object.create(body_base);
    body.url += "/connectors/" + name;
    body.method = "DELETE";

    request(body, (err, resp, body) => cb(err, resp, body));
};

exports.add_connector = (config, cb = def_cb) => {
    var body = Object.create(body_base);
    body.url += "/connectors";
    body.method = "POST";
    body.headers["Content-type"] = "application/json";
    body.json = config;

    request(body, (err, resp, body) => cb(err, resp, body));
};
