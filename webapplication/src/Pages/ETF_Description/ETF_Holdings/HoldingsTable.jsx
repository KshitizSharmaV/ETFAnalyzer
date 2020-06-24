import React, { useState } from "react";
import Card from "react-bootstrap/Card";
import HoldingsPieChart from "./HoldingsPieChart";
import Table from "react-bootstrap/Table";
import { useEffect } from "react";
import Axios from "axios";

const HoldingsTable = (props) => {
  const { ETF, startDate } = props;
  const [tableData, setTableData] = useState({});
  const [order, setTableOrder] = useState([]);
  const [orderType, setOrderType] = useState("ASC");

  useEffect(() => {
    Axios.get(
      `http://localhost:5000/ETfDescription/getHoldingsData/${ETF}/${startDate}`
    ).then(({data}) => {
      setTableData(data);
    }).catch((err) => console.log(err));;

    // if (typeof data === "object") {
    //   const order = Object.keys(data).sort();
    //   setTableOrder(order);
    // }
  }, [ETF, startDate]);

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

  console.log(props);
  return (
    <Card>
      <Card.Header className="text-white BlackHeaderForModal">
        ETF Holdings
      </Card.Header>
      <Card.Body>
        <HoldingsPieChart data={tableData} element={"TickerWeight"} />
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
              {typeof tableData === "object" &&
                order.map((key) => (
                  <tr key={key}>
                    <td>{key}</td>
                    <td>{tableData[key] && tableData[key].TickerName}</td>
                    <td> {tableData[key] && tableData[key].TickerWeight} </td>
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
