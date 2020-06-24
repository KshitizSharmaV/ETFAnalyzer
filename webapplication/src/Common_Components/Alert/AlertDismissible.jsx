import React from "react";
import { Alert, Button } from "react-bootstrap";

const AlertDismissible = ({ variant, message, show, setShow }) => {
  return (
    <>
      <Alert show={show} variant={variant}>
        <Alert.Heading>{message}</Alert.Heading>

        <hr />
        <div className="d-flex justify-content-end">
          <Button onClick={() => setShow(false)} variant="outline-success">
            Close
          </Button>
        </div>
      </Alert>
    </>
  );
};

export default AlertDismissible;
