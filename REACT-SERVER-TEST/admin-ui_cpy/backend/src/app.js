const express    = require("express");
const cors       = require("cors");
const bodyParser = require("body-parser");
const request    = require("request");

const postgres = require("./postgres.js");

const kafka = require("./kafka.js");

const app = express();
app.use(cors()); //TODO muuta turvallisemmaksi!
app.use(bodyParser.json());
app.use(bodyParser.urlencoded( {extended: true} ));

/* for cors */
app.options("*", cors());




/* KAFKA */


/* TOPICS */

app.get("/api/v0/kafka/list_topics", (req, res) => {
    kafka.list_topics( d => res.send({topics: d}) );
});

app.post("/api/v0/kafka/create_topic", (req, res) => {
    kafka.create_topic(req.body.topic);
    //needs error handling
    res.send("done");
});








/* CONNECTORS */

app.post("/api/v0/kafka/add_connector", (req, res) => {
    req.body.config["connector.class"] =
	"io.debezium.connector.postgresql.PostgresConnector";
    req.body.config["slot.name"] = req.body.name;

    postgres.add_connector(req.body.name, JSON.stringify(req.body.config), (err, resp) => {
	if (err) {
	    console.log(err);
	    res.status(500);
	    res.send("database error");
	} else {
	    kafka.add_connector(req.body, (err, resp, body) => {
		if (resp.statusCode != 201) {
		    console.log(resp.body.message);
		    res.status(500);
		    res.send("kafka error");
		} else {
		    res.send("done");
		}
	    });
	}
    });
});

app.post("/api/v0/kafka/update_connector", (req, res) => {
    req.body.config["connector.class"] =
	"io.debezium.connector.postgresql.PostgresConnector";
    req.body.config["slot.name"] = req.body.name;

    postgres.update_connector(req.body.name, JSON.stringify(req.body.config), (err, resp) => {
	if (err) {
            console.log(err);
	    res.status(500);
	    res.send("database error");
	} else {
	    kafka.update_connector(req.body, (err, resp, body) => {
                res.send("done");
	    });
	}
    });
});

app.get("/api/v0/kafka/list_connectors", (req, res) => {
    postgres.list_connectors((err, data) => {
	if (err) {
	    console.log(err);
	    res.status(500);
	    res.send("database error");
	} else {
	    res.send({ connectors: data.rows });
	}
    });
});

app.post("/api/v0/kafka/delete_connector", (req, res) => {
    postgres.delete_connector(req.body.name, (err, _) => {
	if (err) {
	    console.log(err);
	    res.status(500);
	    res.send("database error");
	} else {
	    kafka.delete_connector(req.body.name, (err, resp, body) => {
		if (resp.statusCode != 204) {
		    console.log(JSON.parse(body).message);
		    res.status(500);
		    res.send("kafka error");
		} else {
		    res.send("done");
		}
	    });
	}
    });
});


app.get("/api/v0/kafka/sync_connectors", (req, res) => {
    postgres.list_connectors((err, data) => {
	if (err) {
	    console.log(err);
	    res.status(500);
	    res.send("database error");
	} else {
	    kafka.list_connectors((err, resp, body) => {
		const pg = data.rows;
		var pg_map = {};
		//map to (name => config)
		pg.forEach(o => {
		    const values = Object.values(o);
		    pg_map[values[0]] = values[1];
		});
		const pg_set = new Set(Object.keys(pg_map));
		const kafka_set = new Set(JSON.parse(body));

		//add to kafka connectors that are missing
		var filtered = [...pg_set].filter(v => !kafka_set.has(v));
                filtered.forEach(v => {
		    var cfg = {name: v};
		    cfg.config = JSON.parse(pg_map[v]);
		    cfg.config["connector.class"] =
			"io.debezium.connector.postgresql.PostgresConnector";
		    kafka.add_connector(cfg);
		});

		//remove connectors from kafka that are not in pg
		filtered = [...kafka_set].filter(v => !pg_set.has(v));
                filtered.forEach(v => kafka.delete_connector(v));
	        res.send("done");
            });
	}
    });
});

app.get("/api/v0/kafka/force_sync_connectors", (req, res) => {
    postgres.list_connectors((err, data) => {
	if (err) {
	    console.log(err);
	    res.status(500);
	    res.send("database error");
	} else {
	    kafka.list_connectors((err, resp, body) => {
		const pg = data.rows;
		const kf = JSON.parse(body);

		for (var i = 0; i < kf.length; i++) {
		    kafka.delete_connector(kf[i]);
		}

		for (var i = 0; i < pg.length; i++) {
		    pg[i].config = JSON.parse(pg[i].config);
		    kafka.add_connector(pg[i]);
		};
	    });
	}
    });
});








app.listen(8080);
