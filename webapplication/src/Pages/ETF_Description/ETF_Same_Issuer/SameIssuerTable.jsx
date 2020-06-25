import React, { useState } from "react";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";
import { useEffect } from "react";
import Axios from "axios";

const SameIssuerTable = (props) => {
  const { IssuerName } = props;
  const [tableData, setTableData] = useState({});
  const [order, setTableOrder] = useState([]);
  const [orderType, setOrderType] = useState("ASC");

  useEffect(() => {
    if (IssuerName) {
      Axios.get(
        `http://localhost:5000/ETfDescription/getETFWithSameIssuer/${IssuerName}`
      )
        .then(({ data }) => {
            console.log(data)
          setTableData(data);
        })
        .catch((err) => {
          console.log(err);
        });
    }
  }, [IssuerName]);

  useEffect(() => {
    if (typeof tableData === "object") {
      const order = Object.keys(tableData).sort();
      setTableOrder(order);
    }
  }, [tableData]);

  const changeOrder = () => {
    if (orderType === "ASC") {
      const order = Object.keys(tableData).sort().reverse();
      setOrderType("DSC");
      setTableOrder(order);
    }
    if (orderType === "DSC") {
      const order = Object.keys(tableData).sort();
      setOrderType("ASC");
      setTableOrder(order);
    }
  };

  return (
    <Card>
        
      <Card.Header className="text-white BlackHeaderForModal">
        ETF in Same Issuer
      </Card.Header>
      <Card.Body>
        <div className="DescriptionTable2">
          <Table size="sm" striped bordered hover variant="dark">
            <thead>
              <tr>
                <th className="cursor-pointer" onClick={changeOrder}>
                  Symbol
                </th>
                <th>ETF Name</th>
                <th>TotalAssetsUnderMgmt</th>
              </tr>
            </thead>
            <tbody>
              {typeof tableData === "object" &&
                order.map((key) => (
                  <tr key={key}>
                    <td>{key}</td>
                    <td>{tableData[key] && tableData[key].ETFName}</td>
                    <td>
                      {" "}
                      {tableData[key] &&
                        tableData[key].TotalAssetsUnderMgmt}
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

export default SameIssuerTable;
