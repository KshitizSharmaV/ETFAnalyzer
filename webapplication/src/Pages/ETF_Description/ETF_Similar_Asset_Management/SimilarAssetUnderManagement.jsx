import React, { useState } from "react";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";
import { useEffect } from "react";
import { InputGroup, FormControl } from "react-bootstrap";
import { escapeRegExp } from "lodash";


const SimilarAssetUnderManagement = (props) => {
  const { SimilarTotalAsstUndMgmt } = props;
  const [searchValue, setSearchValue] = useState("");

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

  const handleSearch = (e) => {
    const value = e.target.value;
    setSearchValue(value);
    const re = new RegExp(escapeRegExp(searchValue), "i")
    setTimeout(() => {
      console.log(searchValue)
    }, 5000);
  };

  return (
    <Card>
      <Card.Header className="text-white bg-color-dark">
        Similar Asset under Management
        {/* <InputGroup size="sm">
          <InputGroup.Prepend>
            <InputGroup.Text id="inputGroup-sizing-sm">Search</InputGroup.Text>
          </InputGroup.Prepend>
          <FormControl
            value={searchValue}
            onChange={handleSearch}
            aria-label="Small"
            aria-describedby="inputGroup-sizing-sm"
          />
        </InputGroup> */}
      </Card.Header>
      <Card.Body className="padding-0 bg-color-dark overflow-auto height-50vh font-size-sm">
        <Table size="sm" striped bordered hover variant="dark">
          <thead>
            <tr>
              <th className="cursor-pointer" onClick={changeOrder}>
                Symbol
              </th>
              <th>ETF Name</th>
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
      </Card.Body>
    </Card>
  );
};

export default SimilarAssetUnderManagement;
