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

import moment from 'moment';

import { withRouter } from 'react-router'

const divStyle = {
  paddingTop: '10%',
};

const userPool = new CognitoUserPool({UserPoolId: 'ap-south-1_x8YZmKVyG', ClientId: '2j72c46s52rm3us8rj720tsknd'});

class SignUpFormPage extends React.Component {
  constructor(props){
    super(props);
  }

  state = {
    Email: "",
    Password: "",
    CnfrmPassword : "",
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
              
              <Form.Group value={this.state.CnfrmPassword} onChange={(e) => this.setState({CnfrmPassword: e.target.value})} controlId="formBasicPassword">
                <Form.Label>Password</Form.Label>
                <Form.Control type="password" placeholder="Confirm Password" />
              </Form.Group>
              
              <Button variant="primary" onClick={() => {signUp(this.state.Email, this.state.Password, this.state.CnfrmPassword, this.props.history)}} >
                Submit
              </Button>
            </Form>
          </Col>
        </Row>
      </Container>
    );
  }
};

const signUp = (
  email,
  password,
  ConfirmPassword,
  history,
) => {
  console.log(email);
  console.log(password);
  if (password !== ConfirmPassword){
    alert("Passwords Don't match");
    return;
  }
  const userTimestamp = moment().unix();
  const stringUserTimestamp = userTimestamp.toString();
const user = {
    email,
    password,
  };
const attributesToBeAdded = [
    {
      Name: "email",
      Value: user.email,
    }
  ];
const attrList = attributesToBeAdded.map(
      attr => {
      return new CognitoUserAttribute(attr);
    }
  );
userPool.signUp(email , password, attrList, [], (err, result) => {
    if (err) {
      console.log(err);
      alert(err.message);
      return;
    }
    // setLoading(false);
    if (result && result.user) {
      console.log(result);
      console.log(result.user);
      localStorage.setItem("userID", result.userSub);
      localStorage.setItem("username", email);
      console.log(localStorage.getItem("userID"));
      console.log(localStorage.getItem("username"));
      console.log(history);
      history.push('/EmailVerification  ');
    }
  });
  return;
};

export default withRouter(SignUpFormPage);