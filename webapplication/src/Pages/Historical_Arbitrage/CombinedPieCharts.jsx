import React, { useState } from "react";
import { Button, Modal, Row, Card, Col } from "react-bootstrap";
import { CommonPieChart } from "../../Common_Components/PieChart";

const CombinedPieCharts = (props) => {
  const [show, setShow] = useState(false);

  const handleClose = () => setShow(false);
  const handleShow = () => setShow(true);
  console.log(props);
  return (
    <>
      <Button variant="warning" size="sm" onClick={handleShow}>
        Etf Movers
      </Button>

      <Modal dialogClassName="my-modal" show={show} onHide={handleClose}>
        <Modal.Header closeButton>
          <Modal.Title>Underlying Holdings</Modal.Title>
        </Modal.Header>
        <Modal.Body>
          <Row>
            <Col xs={12} md={12}>
              <Card>
                <Card.Header className="CustomCardHeader text-white CustomBackGroundColor">
                  Holdings
                </Card.Header>
                <Card.Body className="CustomCardBody text-white">
                  <Row>
                    <Col xs={12} md={6}>
                      <center>
                        <h5>Etf Movers</h5>
                      </center>
                      <CommonPieChart
                        data={props.etfmoversDictCount}
                        element={"Count"}
                      />
                    </Col>
                    <Col xs={12} md={6}>
                      <center>
                        <h5>Market Movers</h5>
                      </center>
                      <CommonPieChart
                        data={props.highestChangeDictCount}
                        element={"Count"}
                      />
                    </Col>
                  </Row>
                </Card.Body>
              </Card>
            </Col>
          </Row>
        </Modal.Body>
        <Modal.Footer>
          <Button variant="secondary" onClick={handleClose}>
            Close
          </Button>
          <Button variant="primary" onClick={handleClose}>
            Save Changes
          </Button>
        </Modal.Footer>
      </Modal>
    </>
  );
};

export default CombinedPieCharts;
