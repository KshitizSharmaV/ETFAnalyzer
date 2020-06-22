import React from "react";
import { withRouter } from 'react-router'
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

const userPool = new CognitoUserPool({UserPoolId: 'us-east-1_rLN94MOu0', ClientId: '5fhruc7d6tfo0o1kr41ltfass5'});

const divStyle = {
  paddingTop: '10%',
};

const confirmUser = (
  userId ,
  username ,
  code,
  history
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
      history.push('/Login')
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
              <Button variant="primary" onClick={() => confirmUser(this.state.userId, this.state.username, this.state.Code, this.props.history)}>
                Submit
              </Button>
            </Form>
          </Col>
        </Row>
      </Container>
    );
  }
};


export default withRouter(EmailVerification);

