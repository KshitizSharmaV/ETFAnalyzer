import React from "react";

import Form from 'react-bootstrap/Form'
import Button from 'react-bootstrap/Button'
import Container from 'react-bootstrap/Container'
import Row from 'react-bootstrap/Row'
import Col from 'react-bootstrap/Col'
import {
  AuthenticationDetails,
  CognitoUserPool,
  CognitoUserAttribute,
  CognitoUser,
  CognitoUserSession,
} from "amazon-cognito-identity-js";

import ForgotPassword from './forgotpassword';

const userPool = new CognitoUserPool({UserPoolId: 'ap-south-1_x8YZmKVyG', ClientId: '2j72c46s52rm3us8rj720tsknd'});

const divStyle = {
  paddingTop: '10%',
};

const confirmUser = (
  userId ,
  username ,
  code
) => {
  console.log(code);
  const userData = {
    Username: username,
    Pool: userPool,
  };
  const cognitoUser = new CognitoUser(userData);
  cognitoUser.confirmRegistration(code, true, (err, result) => {
    if (err) {
      alert(err.message);
    }
    if (result === "SUCCESS") {
      // setLoading(false);
      localStorage.setItem("UserAccountVerified", true);
      alert("You are verified now");
    }
  });
};

class EmailVerification extends React.Component {
  constructor(props){
    super(props);
  }

  state = {
    Code: "",
    userId: localStorage.getItem("userID"),
    username: localStorage.getItem("username"),
  }

  render(){
    return (
      <Container fluid>
        <Row className="justify-content-center">
          <Col className="etfArbitrageTable align-item-center" style={divStyle} xs={12} md={3}>
            <Form>
              <Form.Group value={this.state.Code} onChange={(e) => this.setState({Code: e.target.value})}  controlId="formBasicPassword">
                <Form.Label>Enter Verification Code</Form.Label>
                <Form.Control type="password" placeholder="Password" />
              </Form.Group>
              <Button variant="primary" onClick={() => confirmUser(this.state.userId, this.state.username, this.state.Code)}>
                Submit
              </Button>
            </Form>
          </Col>
        </Row>
      </Container>
    );
  }
};


export default EmailVerification;

