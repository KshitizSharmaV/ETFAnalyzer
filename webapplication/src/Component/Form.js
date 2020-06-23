import React, { Component } from 'react';
import Form from 'react-bootstrap/Form'
import Button from 'react-bootstrap/Button'
import DatePicker from "react-datepicker";
import Navbar from 'react-bootstrap/Navbar'
import Nav from 'react-bootstrap/Nav'
import '../static/css/NavStyle.css';
import "react-datepicker/dist/react-datepicker.css";
import { Link, Redirect } from 'react-router-dom';
import { withRouter } from 'react-router'
import { options } from "./options";
import moment from 'moment'
import {
	AuthenticationDetails,
	CognitoUserPool,
	CognitoUserAttribute,
	CognitoUser,
	CognitoUserSession,
} from "amazon-cognito-identity-js";
import Select from "react-dropdown-select";
  
// CSS Modules, react-datepicker-cssmodules.css
import 'react-datepicker/dist/react-datepicker-cssmodules.css';

const userPool = new CognitoUserPool({UserPoolId: 'ap-south-1_x8YZmKVyG', ClientId: '2j72c46s52rm3us8rj720tsknd'});

export function logout(username, history) {
	var cognitoUser = new CognitoUser({
	  Username: username,
	  Pool: userPool
	});
	console.log('signout')
	// call SigOut on User
	cognitoUser.signOut();
	localStorage.removeItem('username')
	localStorage.removeItem("Secret-Token")
    localStorage.removeItem("Expiry-Timestamp")
	history.push('/Login')
	console.log(cognitoUser)
}


class Former extends Component{
	state={
	    arr : options,
	    date: moment(this.props.startDate, 'YYYYMMDD').toDate(),
		ETF: this.props.ETF,
		searchBy: "element",
		labelField: "element",
		valueField: "element",
		color: "#0074D9",
		dropdownPosition: "bottom",
	}

	constructor(props){
		super(props);
	}
	
	// handling date change as well as checking if date lies between 16-17 as data provided has only that
  	changeDate = (selecteddate) => {
  		var DateCopy =  this.state.date;
  		DateCopy = selecteddate;
  		this.setState({
 	   		date:DateCopy,
 	   	});
 	}

  	// Submit funtion to send state to parent to render 
  	submit = () => {
  		var passdate=''
  		passdate = this.state.date.getFullYear() + ("0" + (this.state.date.getMonth() + 1)).slice(-2) + ("0" + this.state.date.getDate()).slice(-2)
		this.props.SubmitFn(this.state.ETF, passdate);
	}

  	
  	FormSelect = (arr) => {
		return arr.map((element, index) => { 
	    	return <option key={index}>{element}</option>
  		})
	}

	select =  (event) => {
		var DateCopy =  this.state.ETF;
  		DateCopy = event.target.values;
  		
		 this.setState({
 	   		ETF:event.target.value
 	   	});
	}
	  
	setValues = selectValues => this.setState({ ETF: selectValues[0].element });

  render(){
	  let button;
	  if (localStorage.getItem('Secret-Token')){
		button = <Button variant="dark" onClick={() => {logout(localStorage.getItem("username"), this.props.history, this.state.isLoggedIn)}}>logout</Button>
		}
		else{
			button = [
			<Button variant="dark" onClick={() => {this.props.history.push('/Login')}}>
				Login
			</Button>,
			<Button variant="dark" onClick={() => {this.props.history.push('/Signup')}}>
				Signup
			</Button>
			]
		}
  	return (
	    <Nav className="bg-dark justify-content-between nav">
	      <Navbar  className="bg-dark">
	        <Nav>
	          <Nav.Item>
	            <Nav.Link style={{color: 'white'}} as={Link} to="/ETF-Description" eventKey="ETF-Description">ETF-Description</Nav.Link>
	          </Nav.Item>
	          <Nav.Item>
	            <Nav.Link style={{color: 'white'}} as={Link} to="/HistoricalArbitrage" eventKey="Historical">Historical Arbitrage</Nav.Link>
	          </Nav.Item>
	          <Nav.Item>
	            <Nav.Link style={{color: 'white'}} as={Link} to="/Live-Arbitrage-single" eventKey="Live-Arbitrage">Live-Arbitrage (Focus)</Nav.Link>
	          </Nav.Item>
	          <Nav.Item>
	            <Nav.Link style={{color: 'white'}} as={Link} to="/Live-Arbitrage" eventKey="Live-Arbitrage">Live-Arbitrage</Nav.Link>
	          </Nav.Item>
	        </Nav>
	      </Navbar>
	      <Navbar className="bg-dark">
	        <Form inline >
	            <Form.Group onChange={this.select}>
				  <Select
				  		style={{color: "black", width: '150px', marginRight: "20px", backgroundColor: "white"}}
						placeholder="Select ETFs"
						labelField={this.state.labelField}
						valueField={this.state.labelField}
						options={options}
						onChange={values => this.setValues(values)}
						noDataLabel="No matches found"
				  />
	            </Form.Group>
	            <DatePicker
	              className="FormInput"
	              selected={this.state.date}
	              onChange={this.changeDate}
	            />
	            <Button variant="primary" onClick={this.submit}>
	              Submit
	            </Button>
	        </Form>
			{button}
	      </Navbar>
	    </Nav>
	  )
	}

}

export default withRouter(Former);