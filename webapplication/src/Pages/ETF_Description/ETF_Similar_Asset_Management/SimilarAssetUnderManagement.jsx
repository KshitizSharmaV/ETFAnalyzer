import React, { useState } from "react";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";
import { useEffect } from "react";
import { InputGroup, FormControl } from "react-bootstrap";
import { escapeRegExp } from "lodash";
import orderBy from "lodash/orderBy"


const SimilarAssetUnderManagement = (props) => {
  const { SimilarTotalAsstUndMgmt } = props;
  const [searchValue, setSearchValue] = useState("");
  const [tableData, setTableData] = useState([]);
  const [orderType, setOrderType] = useState("ASC");


  useEffect(() => {
    const sortedData = orderBy(SimilarTotalAsstUndMgmt, ["etfTicker"], ['asc'])

    setOrderType("DSC");
    setTableData(sortedData)
  }, [SimilarAssetUnderManagement])


  const changeOrder = () => {

    if (orderType === "ASC") {
      const sortedData = orderBy(SimilarTotalAsstUndMgmt, ["etfTicker"], ['asc'])
      console.log(sortedData)
      setOrderType("DSC");
      setTableData(sortedData)
    }
    if (orderType === "DSC") {
      const sortedData = orderBy(SimilarTotalAsstUndMgmt, ["etfTicker"], ['desc'])
      setOrderType("ASC");
      setTableData(sortedData)
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
            {Array.isArray(tableData) &&
              tableData.map(({ ETFName, TotalAssetsUnderMgmt, etfTicker }) => (
                <tr key={etfTicker}>
                  <td>{etfTicker && etfTicker}</td>
                  <td>
                    {ETFName &&
                      ETFName}
                  </td>
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

export default SimilarAssetUnderManagement;
