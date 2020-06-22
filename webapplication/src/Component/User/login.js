import React from "react";
import Form from 'react-bootstrap/Form'
import Button from 'react-bootstrap/Button'
import Container from 'react-bootstrap/Container'
import Row from 'react-bootstrap/Row'
import Col from 'react-bootstrap/Col'
import Modal from 'react-bootstrap/Modal'
import {
  AuthenticationDetails,
  CognitoUserPool,
  CognitoUserAttribute,
  CognitoUser,
  CognitoUserSession,
} from "amazon-cognito-identity-js";

import { withRouter } from 'react-router';

const divStyle = {
  paddingTop: '10%',
};

const userPool = new CognitoUserPool({UserPoolId: 'us-east-1_rLN94MOu0', ClientId: '5fhruc7d6tfo0o1kr41ltfass5'});

export const signIn = (
  email,
  password,
  history
) => {
  const authData = {
    Username: email,
    Password: password,
  };
  const authDetails = new AuthenticationDetails(authData);
  const userData = {
    Username: email,
    Pool: userPool,
  };
  const cognitoUser = new CognitoUser(userData);
  cognitoUser.authenticateUser(authDetails, {
    onSuccess(result) {
      localStorage.setItem("username", email);
      localStorage.setItem("Secret-Token", result.getIdToken().jwtToken)
      localStorage.setItem("Expiry-Timestamp", result.getIdToken().getExpiration())
      localStorage.setItem("TimeStamp", result.idToken.payload["custom:timestamp"] || 0);
      console.log(result.idToken.payload.sub);
      
      history.push("/Live-Arbitrage");  // or whatever route you want a signed in user to be redirected to
    },
    onFailure(err) {
      alert(err.message);
    },
  });
  return;
};

export function resetPassword(username) {
  var cognitoUser = new CognitoUser({
    Username: username,
    Pool: userPool
  });

  // call forgotPassword on cognitoUser
  cognitoUser.forgotPassword({
    onSuccess: function(result) {
      console.log('call result: ' + result);
    },
    onFailure: function(err) {
      alert(err);
    },
    inputVerificationCode() { // this is optional, and likely won't be implemented as in AWS's example (i.e, prompt to get info)
      var verificationCode = prompt('Please input verification code ', '');
      var newPassword = prompt('Enter new password ', '');
      cognitoUser.confirmPassword(verificationCode, newPassword, this);
    }
  });
}

// confirmPassword can be separately built out as follows...  
// export function confirmPassword(username, verificationCode, newPassword) {
//   var cognitoUser = new CognitoUser({
//     Username: username,
//     Pool: userPool
//   });

//   return new Promise((resolve, reject) => {
//     cognitoUser.confirmPassword(verificationCode, newPassword, {
//       onFailure(err) {
//         reject(err);
//       },
//       onSuccess() {
//         resolve();
//       },
//     });
//   });
// }

class SignInFormPage extends React.Component {
  constructor(props){
    super(props);
  }

  state = {
    Email: "",
    Password: "",
    Modal: false,
  }

  Forgot(email){
    console.log(email);
    console.log(userPool);
    var cognitoUser = new CognitoUser({
      Username: email,
      Pool: userPool
    });
  
    // call forgotPassword on cognitoUser
    cognitoUser.forgotPassword({
      onSuccess: function(result) {
        console.log('call result: ' + result);
      },
      onFailure: function(err) {
        alert(err.message);
      },
      inputVerificationCode() { // this is optional, and likely won't be implemented as in AWS's example (i.e, prompt to get info)
        var verificationCode = prompt('Please input verification code ', '');
        var newPassword = prompt('Enter new password ', '');
        cognitoUser.confirmPassword(verificationCode, newPassword, this);
      }
    });
  }

  render(){
    return (
      <Container fluid>
        <Row className="justify-content-center">
          <Col className="etfArbitrageTable align-item-center" style={divStyle} xs={12} md={3}>
            <Form>
              <Form.Group controlId="formBasicEmail">
                <Form.Label>Email address</Form.Label>
                <Form.Control value={this.state.Email} onChange={(e) => this.setState({Email: e.target.value})} type="email" placeholder="Enter email" />
                <Form.Text className="text-muted">
                  We'll never share your email with anyone else.
                </Form.Text>
              </Form.Group>
    
              <Form.Group value={this.state.Password} onChange={(e) => this.setState({Password: e.target.value})} controlId="formBasicPassword">
                <Form.Label>Password</Form.Label>
                <Form.Control type="password" placeholder="Password" />
              </Form.Group>

              <Form.Group controlId="formBasicCheckbox">
                <Form.Check type="checkbox" label="Check me out" />
              </Form.Group>
              
              <Button variant="primary" onClick={() => {signIn(this.state.Email, this.state.Password, this.props.history)}} >
                Submit
              </Button>
              <br />
              <br />

              <Button variant="primary" onClick={() => {this.Forgot(this.state.Email)}} >
                Forgot Password
              </Button>
            </Form>
          </Col>
        </Row>
       </Container>
    );
  }
};

export default withRouter(SignInFormPage);