import React from "react";

import Form from 'react-bootstrap/Form'
import Button from 'react-bootstrap/Button'
import Container from 'react-bootstrap/Container'
import Row from 'react-bootstrap/Row'
import Col from 'react-bootstrap/Col'
import {Link} from 'react-router-dom'

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

const userPool = new CognitoUserPool({UserPoolId: 'us-east-1_rLN94MOu0', ClientId: '5fhruc7d6tfo0o1kr41ltfass5'});

class SignUpFormPage extends React.Component {
  constructor(props){
    super(props);
  }

  state = {
    Name:"",
    Email: "",
    Phone: "",
    Password: "",
    CnfrmPassword : "",
  }

  render(){
    return (
      <Container fluid>
        <Row className="justify-content-center">
          <Col className="etfArbitrageTable align-item-center" style={divStyle} xs={12} md={3}>
            
            <Form>
              <Form.Group controlId="formBasicName">
                <Form.Label style={{color:'white'}}>Full Name</Form.Label>
                <Form.Control value={this.state.Name} onChange={(e) => this.setState({Name: e.target.value})} type="name" placeholder="Enter your Full Name" />
              </Form.Group>
              <Form.Group controlId="formBasicEmail">
                <Form.Label style={{color:'white'}}>Email address</Form.Label>
                <Form.Control value={this.state.Email} onChange={(e) => this.setState({Email: e.target.value})} type="email" placeholder="Enter email" />
                <Form.Text className="text-muted">
                  We'll never share your email with anyone else.
                </Form.Text>
              </Form.Group>

              <Form.Group controlId="formBasicPhone">
                <Form.Label style={{color:'white'}}>Phone Number</Form.Label>
                <Form.Control value={this.state.Phone} onChange={(e) => this.setState({Phone: e.target.value})} type="text" placeholder="Enter Phone No." />
                <Form.Text className="text-muted">
                  Country Code followed by phone number.<br/>
                  Eg. : +14325551212.<br/>
                  We'll never share your phone number with anyone else.
                </Form.Text>
              </Form.Group>
  
              <Form.Group value={this.state.Password} onChange={(e) => this.setState({Password: e.target.value})} controlId="formBasicPassword">
                <Form.Label style={{color:'white'}}>Password</Form.Label>
                <Form.Control type="password" placeholder="Password" />
                <Form.Text className="text-muted">
                  Password must contain : <br/>
                  Must be 8 characters or longer<br/>
                  Numbers(0-9)<br/>
                  Uppercase Letters(A-Z)<br/>
                  Lowercase Letters(a-z)<br/>
                  Special Characters(!,@,#,$,%...etc.)
                </Form.Text>
              </Form.Group>
              
              <Form.Group value={this.state.CnfrmPassword} onChange={(e) => this.setState({CnfrmPassword: e.target.value})} controlId="formBasicPassword">
                <Form.Label style={{color:'white'}}>Confirm Password</Form.Label>
                <Form.Control type="password" placeholder="Confirm Password" />
              </Form.Group>
              
              <Button variant="primary" onClick={() => {signUp(this.state.Name, this.state.Email, this.state.Phone, this.state.Password, this.state.CnfrmPassword, this.props.history)}} >
                Submit
              </Button>
              <Col  style={{color:'white'}} lg={24}>
                Or <Link to="/login">Login to your account!</Link>
              </Col>
            </Form>
          </Col>
        </Row>
      </Container>
    );
  }
};

const signUp = (
  name,
  email,
  phone,
  password,
  ConfirmPassword,
  history,
) => {
  console.log(name);
  console.log(email);
  console.log(password);
  console.log(phone);
  if (password !== ConfirmPassword){
    alert("Passwords Don't match");
    return;
  }
  if (!phone){
    alert("Please provide your phone number")
    return;
  }
  const userTimestamp = moment().unix();
  const stringUserTimestamp = userTimestamp.toString();
const user = {
    name,
    email,
    phone,
    password,
  };
const attributesToBeAdded = [
    {
      Name: "name",
      Value: user.name,
    },
    {
      Name: "email",
      Value: user.email,
    },
    {
      Name: "phone_number",
      Value: user.phone,
    }
  ];
const attrList = attributesToBeAdded.map(
      attr => {
      return new CognitoUserAttribute(attr);
    }
  );
userPool.signUp(email, password, attrList, [], (err, result) => {
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
      history.push('/EmailVerification');
    }
  });
  return;
};

export default withRouter(SignUpFormPage);