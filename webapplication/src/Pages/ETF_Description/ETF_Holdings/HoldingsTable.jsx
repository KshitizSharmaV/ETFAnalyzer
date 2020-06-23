import React, { useState } from "react";
import Card from "react-bootstrap/Card";
import HoldingsPieChart from "./HoldingsPieChart";
import Table from "react-bootstrap/Table";
import { useEffect } from "react";

const HoldingsTable = (props) => {
  const { data } = props;
  const [order, setTableOrder] = useState([]);
  const [orderType, setOrderType] = useState("ASC");

  useEffect(() => {
    if (typeof data === "object") {
      const order = Object.keys(data).sort();
      setTableOrder(order);
    }
  }, [data]);

  const changeOrder = () => {
    if (orderType === "ASC") {
      const order = Object.keys(data).sort().reverse();
      setOrderType("DSC");
      setTableOrder(order);
    }
    if (orderType === "DSC") {
      const order = Object.keys(data).sort();
      setOrderType("ASC");
      setTableOrder(order);
    }
  };

  console.log(props);
  return (
    <Card>
      <Card.Header className="text-white BlackHeaderForModal">
        ETF Holdings
      </Card.Header>
      <Card.Body>
        <HoldingsPieChart data={data} element={"TickerWeight"} />
        <div className="DescriptionTable2">
          <Table size="sm" striped bordered hover variant="dark">
            <thead>
              <tr>
                <th className="cursor-pointer" onClick={changeOrder}>
                  Symbol
                </th>
                <th>TickerName</th>
                <th>TickerWeight</th>
              </tr>
            </thead>
            <tbody>
              {typeof props.data === "object" &&
                order.map((key) => (
                  <tr key={key}>
                    <td>{key}</td>
                    <td>{data[key] && data[key].TickerName}</td>
                    <td> {data[key] && data[key].TickerWeight} </td>
                  </tr>
                ))}
            </tbody>
          </Table>
        </div>
      </Card.Body>
    </Card>
  );
};

export default HoldingsTable;
