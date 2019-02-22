const { Pool } = require("pg");

const def_cb = () => {};

const pool = new Pool({
    user: "node_app",
    database: "backend",
    password: "node_app",
    max: 5
});

pool.connect();

exports.list_connectors = (cb = def_cb) => {
    const query = "SELECT * FROM connectors;";
    pool.query(query, (err, res) => cb(err, res));
};

exports.add_connector = (name, config, cb = def_cb) => {
    const query = "INSERT INTO connectors VALUES ($1, $2);";
    pool.query(query, [name, config], (err, res) => cb(err, res));
};

exports.update_connector = (name, config, cb = def_cb) => {
    const query = "UPDATE connectors SET config=($1) WHERE name=($2);";
    pool.query(query, [config, name], (err, res) => cb(err, res));
};

exports.delete_connector = (name, cb = def_cb) => {
    const query = "DELETE FROM connectors WHERE name=$1;";
    pool.query(query, [name], (err, res) => cb(err,res));
};
