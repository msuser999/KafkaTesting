# Starting the backend
Install packages with

```npm install```

Start the backend with

```npm start```




# API calls
## Kafka
* [List topics](#list-topics)
* [Create topic](#create-topic)
* [Create connector](#create-connector)
* [Update connector](#update-connector)
* [List connectors](#list-connectors)
* [Delete connector](#delete-connector)

### List topics
Endpoint: ```/api/v0/kafka/list_topics```

Method: ```GET```

Response:
```javascript
{
  topics: []
}
```

### Create topic
Endpoint: ```/api/v0/kafka/create_topic```

Method: ```POST```

Parameters:
```javascript
{
  topic: string
}
```

Response: None (todo)

### Create connector
Endpoint: ```/api/v0/kafka/add_connector```

Method: ```POST```

Parameters:
```javascript
{
  name: string,
  config: {
    database.hostname: string,
    database.port: string,
    database.user: string,
    database.password: string,
    database.dbname: string,
    //bunch of optional parameters which can be added
  }
}
```
all parameters listed [here](https://debezium.io/docs/connectors/postgresql/#connector-properties)

Response: none (todo)

### Update connector
Endpoint: ```/api/v0/kafka/update_connector```

Method: ```POST```

Parameters: same as with creating

Response: none (todo)

### List connectors
Endpoint: ```/api/v0/kafka/list_connectors```

Method: ```GET```

Response:
```javascript
{
  connectors: [
    {name: string, config: string},
    ...
  ]
}
```

### Delete connector
Endpoint: ```/api/v0/kafka/delete_connector```

Method: ```POST```

Parameters:
```javascript
{
  name: string
}
```

Response: none (todo)
