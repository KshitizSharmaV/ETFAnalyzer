import React, { useState } from "react";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";
import { useEffect } from "react";
import Axios from "axios";

const SameIndustryTable = (props) => {
  const { EtfDbCategory } = props;
  const [tableData, setTableData] = useState({});
  const [order, setTableOrder] = useState([]);
  const [orderType, setOrderType] = useState("ASC");

  useEffect(() => {
    if (EtfDbCategory) {
      Axios.get(
        `http://localhost:5000/ETfDescription/getETFsWithSameETFdbCategory/${EtfDbCategory}`
      )
        .then(({ data }) => {
          console.log(data);
          setTableData(data);
        })
        .catch((err) => {
          console.log(err);
        });
    }
  }, [EtfDbCategory]);

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
      <Card.Header className="text-white bg-color-dark">
        ETF in same industry : Technology Equities
      </Card.Header>
      <Card.Body className="padding-0 bg-color-dark overflow-auto height-50vh font-size-sm">
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
                    {tableData[key] && tableData[key].TotalAssetsUnderMgmt}{" "}
                  </td>
                </tr>
              ))}
          </tbody>
        </Table>
      </Card.Body>
    </Card>
  );
};

export default SameIndustryTable;
