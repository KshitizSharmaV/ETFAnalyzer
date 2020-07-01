import React, { useState } from "react";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";
import { useEffect } from "react";
import Axios from "axios";
import orderBy from "lodash/orderBy"


const SameIndustryTable = (props) => {
  const { EtfDbCategory } = props;
  const [tableData, setTableData] = useState([]);
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

  // useEffect(() => {
  //   if (typeof tableData === "object") {
  //     const order = Object.keys(tableData).sort();
  //     setTableOrder(order);
  //   }
  // }, [tableData]);

  const changeOrder = () => {
    if (orderType === "ASC") {
      const sortedData = orderBy(tableData, ["etfTicker"], ['asc'])

      setOrderType("DSC");
      setTableData(sortedData)
    }
    if (orderType === "DSC") {
      const sortedData = orderBy(tableData, ["etfTicker"], ['desc'])
      setOrderType("ASC");
      setTableData(sortedData)
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
            {Array.isArray(tableData) &&
              tableData.map(({ ETFName, TotalAssetsUnderMgmt, etfTicker }) => (
                <tr key={etfTicker}>
                  <td>{etfTicker && etfTicker}</td>
                  <td>{ETFName && ETFName}</td>
                  <td>
                    {TotalAssetsUnderMgmt && TotalAssetsUnderMgmt}
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
