import React, { useState } from "react";
import { Button, Modal } from "react-bootstrap";
import { CommonPieChart } from "../../../Common_Components/PieChart";

const PieChartModal = ({ data, element }) => {
  const [show, setShow] = useState(false);
  const handleClose = () => setShow(false);
  const handleShow = () => setShow(true);
  return (
    <>
      <Button
        className="margin-left-auto padding-1px"
        size="sm"
        variant="primary"
        onClick={handleShow}
      >
        PieChart
      </Button>

      {show && (
        <Modal show={show} onHide={handleClose}>
          <Modal.Header closeButton>
            <Modal.Title>ETF Holdings</Modal.Title>
          </Modal.Header>
          <Modal.Body className="margin-auto">
            <CommonPieChart data={data} element="TickerWeight" />
          </Modal.Body>
          <Modal.Footer>
            <Button variant="secondary" onClick={handleClose}>
              Close
            </Button>
          </Modal.Footer>
        </Modal>
      )}
    </>
  );
};

export default PieChartModal;
