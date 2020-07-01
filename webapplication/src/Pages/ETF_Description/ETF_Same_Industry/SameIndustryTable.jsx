import React, { useState } from "react";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";
import { useEffect } from "react";
import Axios from "axios";
import orderBy from "lodash/orderBy";
import escapeRegExp from "lodash/escapeRegExp";
import filter from "lodash/filter";
import debounce from "lodash/debounce";

const SameIndustryTable = (props) => {
  const { EtfDbCategory } = props;
  const [tableData, setTableData] = useState([]);
  const [orderType, setOrderType] = useState("ASC");
  const [searchString, setSearchString] = useState("");
  const [filterData, setFilterData] = useState([]);

  useEffect(() => {
    if (EtfDbCategory) {
      Axios.get(
        `http://localhost:5000/ETfDescription/getETFsWithSameETFdbCategory/${EtfDbCategory}`
      )
        .then(({ data }) => {
          console.log(data);
          setTableData(data);
          setFilterData(data);
        })
        .catch((err) => {
          console.log(err);
        });
    }
  }, [EtfDbCategory]);

  useEffect(() => {
    setTimeout(() => {
      if (searchString < 1) {
        return setFilterData(tableData);
      }

      const re = new RegExp(escapeRegExp(searchString), "i");
      const isMatch = (result) => re.test(result.etfTicker);
      setFilterData(filter(tableData, isMatch));
    }, 300);
  }, [searchString]);

  const handleSearchChange = (e) => {
    setSearchString(e.target.value);
  };

  const changeOrder = () => {
    if (orderType === "ASC") {
      const sortedData = orderBy(filterData, ["etfTicker"], ["asc"]);

      setOrderType("DSC");
      setFilterData(sortedData);
    }
    if (orderType === "DSC") {
      const sortedData = orderBy(filterData, ["etfTicker"], ["desc"]);
      setOrderType("ASC");
      setFilterData(sortedData);
    }
  };

  return (
    <Card>
      <Card.Header className="text-white bg-color-dark flex-row">
        ETF in Same Industry : Technology Equities
        <input
          className="margin-left-auto d-inline-block"
          name="search"
          onChange={debounce(handleSearchChange, 500, { leading: true })}
          value={searchString}
        />
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
            {Array.isArray(filterData) &&
              filterData.map(({ ETFName, TotalAssetsUnderMgmt, etfTicker }) => (
                <tr key={etfTicker}>
                  <td>{etfTicker && etfTicker}</td>
                  <td>{ETFName && ETFName}</td>
                  <td>{TotalAssetsUnderMgmt && TotalAssetsUnderMgmt}</td>
                </tr>
              ))}
          </tbody>
        </Table>
      </Card.Body>
    </Card>
  );
};

export default SameIndustryTable;
