import React, { Component } from 'react'
import axios from '../axios.js'

class ConnectorSync extends Component {

  click = id => {
    const force = '/api/v0/kafka/force_sync_connectors'
    const sync = '/api/v0/kafka/sync_connectors'
    const url = (id === 0) ? sync : force
    axios.get(url)
  }

  render(){
    return(
      <div>
        <h2>Connector syncronization</h2>
        <button
          type="button"
          onClick={() => this.click(0)}
          title="Add missing connectors to kafka">
            Sync
        </button>
        <button
          type="button"
          onClick={() => this.click(1)}
          title="Delete all connectors from kafka and remake them">
            Force
        </button>
      </div>
    )
  }
}

export default ConnectorSync
