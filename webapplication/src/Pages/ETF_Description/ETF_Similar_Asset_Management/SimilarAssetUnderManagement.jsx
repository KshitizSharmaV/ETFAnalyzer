import React, { useState } from "react";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";
import { useEffect } from "react";

const SimilarAssetUnderManagement = (props) => {
  const { SimilarTotalAsstUndMgmt } = props;

  const [order, setTableOrder] = useState([]);
  const [orderType, setOrderType] = useState("ASC");

  useEffect(() => {
    if (typeof SimilarTotalAsstUndMgmt === "object") {
      const order = Object.keys(SimilarTotalAsstUndMgmt).sort();
      setTableOrder(order);
    }
  }, [SimilarTotalAsstUndMgmt]);

  const changeOrder = () => {
    if (orderType === "ASC") {
      const order = Object.keys(SimilarTotalAsstUndMgmt).sort().reverse();
      setOrderType("DSC");
      setTableOrder(order);
    }
    if (orderType === "DSC") {
      const order = Object.keys(SimilarTotalAsstUndMgmt).sort();
      setOrderType("ASC");
      setTableOrder(order);
    }
  };

  return (
    <Card>
      {console.log(SimilarTotalAsstUndMgmt, order)}
      <Card.Header className="text-white BlackHeaderForModal">
        Similar Asset under Management
      </Card.Header>
      <Card.Body>
        <div className="DescriptionTable2">
          <Table size="sm" striped bordered hover variant="dark">
            <thead>
              <tr>
                <th className="cursor-pointer" onClick={changeOrder}>Symbol</th>
                <th >
                  ETF Name
                </th>
                <th>Total Asset</th>
              </tr>
            </thead>
            <tbody>
              {typeof SimilarTotalAsstUndMgmt === "object" &&
                order.map((key) => (
                  <tr key={key}>
                    <td>{key && key}</td>
                    <td>
                      {SimilarTotalAsstUndMgmt[key] &&
                        SimilarTotalAsstUndMgmt[key].ETFName}{" "}
                    </td>
                    <td>
                      {SimilarTotalAsstUndMgmt[key] &&
                        SimilarTotalAsstUndMgmt[key].TotalAssetsUnderMgmt}{" "}
                    </td>
                  </tr>
                ))}
            </tbody>
          </Table>
        </div>
      </Card.Body>
    </Card>
  );
};

export default SimilarAssetUnderManagement;
