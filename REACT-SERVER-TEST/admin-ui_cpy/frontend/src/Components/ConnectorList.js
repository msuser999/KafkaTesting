import React, { Component } from 'react'
import axios from '../axios.js'
import ReactModal from 'react-modal'
import ConnectorForm from './ConnectorForm'
import ConnectorSync from './ConnectorSync'

class ConnectorList extends Component {
  constructor(props) {
    super(props)
    this.state = {
      listOfConnectors: [],
      isLoaded: false,
      showModal: false
    }

    this.handleOpenModal = this.handleOpenModal.bind(this)
    this.handleCloseModalDeleteConfirmed = this.handleCloseModalDeleteConfirmed.bind(this)
    this.handleCloseModalCancelled = this.handleCloseModalCancelled.bind(this)
  }

  modalStyles = {
    content : {
    top                   : '50%',
    left                  : '50%',
    right                 : 'auto',
    bottom                : 'auto',
    marginRight           : '-50%',
    transform             : 'translate(-50%, -50%)'
    }
  }

  handleOpenModal = () => {
    this.setState({ showModal: true })
  }

  handleCloseModalDeleteConfirmed = () => {
    this.setState({ showModal: false })
    this.refreshList()
  }

  handleCloseModalCancelled = () => {
    this.setState({ showModal: false })
  }

  getConnectors = () => {
    const currentComponent = this
    axios
      .get('/api/v0/kafka/list_connectors')
      .then(function(res){
        const connectors = res.data.connectors.map(connector => connector.name)
        currentComponent.setState({
          listOfConnectors: connectors,
          isLoaded: true
        })
      })
      .catch((err) => alert(err))
  }

  componentDidMount = () => {
    ReactModal.setAppElement('body')
    this.getConnectors()
  }

  deleteConnector = (connectorName) => {
    const url =  '/api/v0/kafka/delete_connector/'
    axios.post(url, {
      name: connectorName
    })
    .then(function (response) {
    })
    .catch(function (error) {
      alert(error)
    })
    this.getConnectors()
  }

  refreshList = () => {
    this.setState({ isLoaded: false })
  }

  render(){

    var listItems
    if (!this.state.isLoaded) listItems = <p>Loading</p>
    else if (!this.state.listOfConnectors.length){
      listItems = <p>There is no connectors yet</p>
    }
    else {
      listItems = this.state.listOfConnectors.map((connectorName, i) => (
         <p key={i}>{connectorName}
          <button onClick={() => (
            this.handleOpenModal()
          )}>
          Delete
          </button>
          <ReactModal
             isOpen={this.state.showModal}
             style={ this.modalStyles }
          >
            <p>Delete {connectorName}?</p>
            <button onClick={() => {
              this.deleteConnector(connectorName)
              this.handleCloseModalDeleteConfirmed()
            }}>Delete</button>
            <button onClick={ this.handleCloseModalCancelled }>Cancel</button>
          </ReactModal>
        </p>
       ))
    }
    return(
      <div>
        <h2>Connectors</h2>
        {listItems}
        <ConnectorForm />
        <ConnectorSync />
      </div>
    )
  }
}

export default ConnectorList
