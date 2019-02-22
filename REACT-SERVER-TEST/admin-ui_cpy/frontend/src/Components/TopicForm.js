import React, { Component } from 'react'
import TopicList from './TopicList'
import axios from '../axios.js'

class TopicForm extends Component {

  inputElement = React.createRef()

  addTopic = e => {
     //e.preventDefault()
     const topicName = e.target.elements.topicname.value
     const url =  '/api/v0/kafka/create_topic/'
     axios.post(url, {
       topic: topicName
     })
     .then(function (response) {
       console.log(response)
     })
     .catch(function (error) {
       console.log(error)
    })
  }

  render(){
    return(
      <div>
        <TopicList />
        <h2>Add Topic</h2>
        <form onSubmit={this.addTopic}>
          <input
            placeholder="Topic"
            name="topicname"
            ref={this.inputElement}
          />
          <button type="submit">Add Topic</button>
        </form>
      </div>
    )
  }
}

export default TopicForm
