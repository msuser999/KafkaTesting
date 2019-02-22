import React, { Component } from 'react'
import axios from '../axios.js'
//import ConnectorList from './ConnectorList'

class ConnectorForm extends Component {

  constructor(props) {
    super(props)
    this.state = {
      name: '',
      hostname: '',
      port: '',
      user: '',
      password: '',
      dbname: ''
    }
    this.handleSubmit = this.handleSubmit.bind(this);
  }

  handleSubmit(event) {
    const url = '/api/v0/kafka/add_connector'
    axios.post(url, {
        'name': this.state.name,
         'config': {
          'database.hostname': this.state.hostname,
          'database.port': this.state.port,
          'database.user': this.state.user,
          'database.password': this.state.password,
          'database.dbname': this.state.dbname
        }
    })
  }

  render(){
    return(
      <div>
        <h2>Add Connector</h2>
        <form onSubmit={this.handleSubmit}>
        <label>
          Connector Name
          <br />
          <input
            placeholder="name"
            name="name"
            value={this.state.name}
            onChange={(e)=>this.setState({ name: e.currentTarget.value })}
          />
        </label>
        <br />
        <label>
          Host name
          <br />
          <input
            placeholder="hostname"
            name="hostname"
            value={this.state.hostname}
            onChange={(e)=>this.setState({ hostname: e.currentTarget.value })}
          />
        </label>
        <br />
        <label>
          Port
          <br />
          <input
            placeholder="port"
            name="port"
            value={this.state.port}
            onChange={(e)=>this.setState({ port: e.currentTarget.value })}
          />
        </label>
        <br />
        <label>
          User
          <br />
          <input
            placeholder="user"
            name="user"
            value={this.state.user}
            onChange={(e)=>this.setState({ user: e.currentTarget.value })}
          />
        </label>
        <br />
        <label>
        Password
          <br />
          <input
            type="password"
            placeholder="password"
            name="password"
            value={this.state.password}
            onChange={(e)=>this.setState({ password: e.currentTarget.value })}
          />
        </label>
        <br />
        <label>
        Database Name
        <br />
        <input
          placeholder="database name"
          name="dbname"
          value={this.state.dbname}
          onChange={(e)=>this.setState({ dbname: e.currentTarget.value })}
        />
        </label>
        <br />
          <button type="submit">Add Connector</button>
        </form>
      </div>
    )
  }
}

export default ConnectorForm
