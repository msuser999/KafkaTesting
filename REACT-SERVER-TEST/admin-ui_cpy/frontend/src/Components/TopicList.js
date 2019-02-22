import React, { Component } from 'react'
import axios from '../axios.js'

class TopicList extends Component {
  constructor(props) {
    super(props)
    this.state = {
      listOfTopics: [],
      isLoaded: false
    }
  }

  getTopics = () => {
    const currentComponent = this
    console.log('Topic list: getTopics')
    axios
      .get('/api/v0/kafka/list_topics')
      .then(function(res){
        console.log(res.data.topics) //toimii
        currentComponent.setState({
          listOfTopics: res.data.topics,
          isLoaded: true
        })
      })
      .catch((err) => console.log(err))
  }

  componentDidMount = () => {
    this.getTopics()
  }


  render(){
    var listItems
    if (!this.state.isLoaded) listItems = <p>Loading</p>
    else if (!this.state.listOfTopics.length) listItems = <p>There is no topics yet</p>
    else {
      listItems = this.state.listOfTopics.map((topicName, i) => <p key={i}>{topicName}<button>Delete</button></p>)
    }
    return(
      <div>
        <h2>Topics</h2>
        {listItems}
      </div>
    )
  }
}

export default TopicList
