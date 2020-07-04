import React, { Component } from 'react';
import ReactDOM from 'react-dom';
import { Route, BrowserRouter as Router, Redirect } from 'react-router-dom'
import Container from 'react-bootstrap/Container'

import Former from './Component/Form.js';
import Live_Arbitrage from './Component/Live-Arbitrage';
import Live_Arbitrage_Single from './Component/Live-Arbitrage-Single';
import ML from './Component/Machine-Learning';
import SignInFormPage from './Component/User/login';
import SignUpFormPage from './Component/User/signup';
import EmailVerification from './Component/User/emailverification';
import { createBrowserHistory } from "history";
import {
	AuthenticationDetails,
	CognitoUserPool,
	CognitoUserAttribute,
	CognitoUser,
	CognitoUserSession,
} from "amazon-cognito-identity-js";


// StylesSheets
import './static/css/style.css';
import { ETF_Description } from './Pages/ETF_Description';
import { HistoricalArbitragee } from './Pages/Historical_Arbitrage/index.js';

const history = createBrowserHistory();

const userPool = new CognitoUserPool({UserPoolId: 'ap-south-1_x8YZmKVyG', ClientId: '2j72c46s52rm3us8rj720tsknd'});
const cognitoUser = null
try {
  cognitoUser = new CognitoUser({
    Username: localStorage.getItem("username"),
    Pool: userPool
  });	
}
catch(e){
}

const PrivateRoute = ({ component: Component, path, ...rest }) => (
  <Route path={path} {...rest} render={(props) => (
    localStorage.getItem("Secret-Token") ? ((parseInt(localStorage.getItem("Login-TimeStamp"))+8*60*60)>Math.floor(new Date().getTime()/1000)?
      <Component {...props} {...rest}/>
      : <Redirect to='/Login' />) : <Redirect to='/Login' />
  )} />
)

class App extends Component {
  
  state={
    ETF:'XLK',
    startDate:'20200608'
  };

  componentDidMount() {
		this.setState({
			ETF:'XLK',
	   		startDate:'20200608'
		});
  }

  SubmitFn = (etfname, newdate) => {	
    console.log("Change ETF Name & Date");
    
    let ETFcopy = this.state.ETF;
    let startDatecopy = this.state.startDate;

    ETFcopy=etfname
    startDatecopy=newdate

    console.log(etfname);
    console.log(newdate);
      
    this.setState({
      ETF:ETFcopy,
      startDate:startDatecopy
    });
  };

  SubmitNewETF = (etfName) => {
    this.setState({ETF: etfName});
  }


  render(){
  	return (
    <Router history={history} >
      <div className="Container">
        <div>
          <div className="Form">
            <Former SubmitFn={this.SubmitFn} ETF={this.state.ETF} startDate={this.state.startDate}/>
          </div>
        </div>
      </div>
      <Container fluid style={{'backgroundColor':'#292b2c'}}>
        
        <PrivateRoute path="/ETF-Description" startDate={this.state.startDate} ETF={this.state.ETF} submitFn={this.SubmitNewETF} component={ETF_Description}/>
        <PrivateRoute path="/Live-Arbitrage-Single" component={Live_Arbitrage_Single} ETF={this.state.ETF} />
        <PrivateRoute path="/Live-Arbitrage" component={Live_Arbitrage} ETF={this.state.ETF} />
        <PrivateRoute path="/newhistoricalarbitrage" startDate ={this.state.startDate} ETF={this.state.ETF} component={HistoricalArbitragee} ETF={this.state.ETF} />

        <Route path="/SignUp" render={() => <SignUpFormPage />} />
        <Route path="/Login" component={SignInFormPage} />
        <Route path="/EmailVerification" component={EmailVerification} />
      </Container>
    </ Router>
    );
  }

}

ReactDOM.render(<App />,document.getElementById('root'));
