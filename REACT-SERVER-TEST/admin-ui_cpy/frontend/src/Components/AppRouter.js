import React from "react"
import {BrowserRouter as Router, Route, Link } from "react-router-dom"
import TopicForm from './TopicForm'
//import TopicList from './TopicList'
import ConnectorList from './ConnectorList'

const Topics = () => <div><TopicForm /></div>
const Connectors = () => <div><ConnectorList /></div>

const AppRouter = () => (
    <Router>
      <div>
        <nav>
          <li>
            <Link to="/">Topics</Link>
          </li>
          <li>
            <Link to="/connectors/">Connectors</Link>
          </li>
        </nav>
        <hr/>
        <Route path="/" exact component={Topics} />
        <Route path="/connectors/" exact component={Connectors} />
      </div>
    </Router>
)

export default AppRouter
