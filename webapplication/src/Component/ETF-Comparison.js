import React, { useState, useEffect } from "react";
import PieChart from "./PieChart";
import AppTable from "./Table.js";
import "../static/css/Description.css";
import Container from "react-bootstrap/Container";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import ChartComponent from "./StockPriceChart";
import Modal from "react-bootstrap/Modal";
import Button from "react-bootstrap/Button";
import axios from "axios";
import Card from "react-bootstrap/Card";

import { tsvParse, csvParse } from "d3-dsv";
import { timeParse } from "d3-time-format";

class ETFComparison extends React.Component {
  constructor(props) {
  	super(props);
    this.state = {
      ETFdata: null,
      PNLdata: null,
      ScatterPlotdata: null,
    };
  }

  componentDidMount() {
    this.fetchETFDescriptionData();
  }

  componentDidUpdate(prevProps, prevState) {
    const condition1 = this.props.ETF !== prevProps.ETF;
    const condition2 = this.props.startDate !== prevProps.startDate;

    if (condition1 || condition2) {
      this.fetchETFDescriptionData();
    }
  }

  fetchETFDescriptionData() {
    axios
      .get(
        `http://localhost:5000/ETFComparison/${this.props.ETF}/${this.props.startDate}`
      )
      .then((res) => {
        console.log(res);
        this.setState({
          //ETFdata: res.data.ETFdata,
          PNLdata: res.data.PNLdata,
          ScatterPlotdata: res.data.ScatterPlotdata,
        });
      });
  }

  render() {
    return (
      <Container fluid className="pt-3">
        <Row>
          <Col xs={12} md={9}>
            <Row>
              <Col xs={12} md={4}>
                <Card>
                  <Card.Header className="text-white BlackHeaderForModal">
                    ETF ETfComparison
                  </Card.Header>
                  <Card.Body>
                    <div className="DescriptionTable2">
                      {this.state.ETFdata != null ? (
                        <AppTable
                          data={this.state.ETFdata}
                          clickableTable={"False"}
                        />
                      ) : (
                        ""
                      )}
                    </div>
                  </Card.Body>
                </Card>
              </Col>

              <Col xs={12} md={4}>
                <Card>
                  <Card.Header className="text-white BlackHeaderForModal">
                    PNL Data
                  </Card.Header>
                  <Card.Body>
                    <div className="DescriptionTable2">
                      {this.state.PNLdata != null ? (
                        <AppTable
                          data={this.state.PNLdata}
                          clickableTable={"False"}
                        />
                      ) : (
                        ""
                      )}
                    </div>
                  </Card.Body>
                </Card>
              </Col>
            </Row>
          </Col>
        </Row>
      </Container>
    );
  }
}

export default ETFComparison;