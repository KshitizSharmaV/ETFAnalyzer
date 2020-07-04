import React from "react";
import Axios from "axios";

import Container from "react-bootstrap/Container";
import Row from "react-bootstrap/Row";
import Button from "react-bootstrap/Button";
import Col from "react-bootstrap/Col";
import Card from "react-bootstrap/Card";
import Modal from "react-bootstrap/Modal";
import Table from "react-bootstrap/Table";
// Code to display chaer
import { tsvParse, csvParse } from "d3-dsv";
import { timeParse } from "d3-time-format";
import "./Styles/arbitrage.css";
import CombinedPieCharts from "./CombinedPieCharts";
import EtfArbitrageTable from "./EtfArbitrageTable";
import HisArbLineChart from "./HisArbLineChart";
import ChartComponent from "../../Component/StockPriceChart";
import AppTable from "../../Component/Table";
import ScatterPlot from "./ScatterPlot";

class HistoricalArbitrage extends React.Component {
  state = {
    etfArbitrageTableData: null,
    historicalArbitrageData: "",
    scatterPlotData: "",
    LoadingStatement: "Loading.. PNL for " + this.props.ETF,
    etfmoversDictCount: null,
    etfPriceData: "",
    ArbitrageCumSum: "",
    highestChangeDictCount: null,
  };
  parseDate = timeParse("%Y-%m-%d %H:%M:%S");

  parseData(parse) {
    return function (d) {
      d.date = parse(d.date);
      d.open = +parseFloat(d.open);
      d.high = +parseFloat(d.high);
      d.low = +parseFloat(d.low);
      d.close = +parseFloat(d.close);
      d.volume = +parseInt(d.volume);

      return d;
    };
  }

  componentDidMount() {
    Axios.get(`/PastArbitrageData/${this.props.ETF}/${this.props.startDate}`)
      .then(({ data }) =>
        this.setState({
          etfArbitrageTableData: JSON.parse(data.etfhistoricaldata),
          PNLStatementForTheDay: JSON.parse(data.PNLStatementForTheDay),
          etfPriceData: {
            data: tsvParse(data.etfPrices, this.parseData(this.parseDate)),
          },
          scatterPlotData: JSON.parse(data.scatterPlotData),
          etfmoversDictCount: JSON.parse(data.etfmoversDictCount),
          highestChangeDictCount: JSON.parse(data.highestChangeDictCount),
          SignalCategorization: <AppTable data={JSON.parse(data.SignalCategorization)}/>,
          ArbitrageCumSum: data.ArbitrageCumSum,
        })
      )
      .then((err) => console.log(err));
  }

  render() {
    return (
      <div>
        <Row>
          <Col className="etfArbitrageTable" xs={12} md={5}>
            <Card>
              <Card.Header className=" text-white bg-color-dark flex-row">
                <span>
                  {this.props.ETF} {this.props.startDate}
                </span>

                <div className="margin-left-auto">
                  <CombinedPieCharts
                    etfmoversDictCount={this.state.etfmoversDictCount}
                    highestChangeDictCount={this.state.highestChangeDictCount}
                  />
                </div>
              </Card.Header>
              <Card.Body className="BlackHeaderForModal">
                <div className="FullPageDiv">
                  <EtfArbitrageTable data={this.state.etfArbitrageTableData} />
                </div>
              </Card.Body>
            </Card>
          </Col>

          <Col xs={12} md={7}>
            <Row>
              <Col xs={12} md={4}>
                <Row>
                  <Col xs={12} md={12}>
                    <Card className="CustomCard">
                      <Card.Header className="CustomCardHeader text-white">
                        PNL
                      </Card.Header>
                      <Card.Body className="CustomCardBody text-white">
                        <Table
                          size="sm"
                          striped
                          bordered
                          hover
                          variant="dark"
                          className="font-size-sm"
                        >
                          <tbody>
                            {typeof this.state.PNLStatementForTheDay ===
                              "object" &&
                              Object.entries(
                                this.state.PNLStatementForTheDay
                              ).map(([key, value]) => (
                                <tr key={key}>
                                  <td>{key}</td>
                                  <td>{value}</td>
                                </tr>
                              ))}
                          </tbody>
                        </Table>
                      </Card.Body>
                    </Card>
                  </Col>

                  <Col xs={12} md={12}>
                    <Card className="CustomCard">
                      <Card.Header className="CustomCardHeader text-white">
                        Arb Time Series
                      </Card.Header>
                      <Card.Body className="CustomCardBody text-white">
                        <HisArbLineChart data={this.state.ArbitrageCumSum} />
                      </Card.Body>
                    </Card>
                  </Col>

                  <Col xs={12} md={12}>
                    <Card className="CustomCard">
                      <Card.Header className="CustomCardHeader text-white">
                        Signal Status
                      </Card.Header>
                        <Card.Body className="CustomCardBody text-white">
                        {this.state.SignalCategorization}
                        </Card.Body>
                    </Card>
                  </Col>

                </Row>
              </Col>

              <Col xs={12} md={8}>
                <Row>
                  <Col xs={12} md={12}>
                    <Card>
                      <Card.Header className="modalCustomHeader text-white CustomBackGroundColor">
                        Price Chart
                      </Card.Header>
                      <Card.Body className="BlackHeaderForModal">
                        <ChartComponent data={this.state.etfPriceData} />
                      </Card.Body>
                    </Card>
                  </Col>
                  <Col xs={12} md={12}>
                    <Card className="CustomCard">
                      <Card.Header className="CustomCardHeader text-white">
                        ETF Change % Vs NAV change %
                      </Card.Header>
                      <Card.Body className="CustomCardBody text-white">
                        <ScatterPlot data={this.state.scatterPlotData} />
                      </Card.Body>
                    </Card>
                  </Col>
                </Row>
              </Col>
            </Row>
          </Col>
        </Row>
      </div>
    );
  }
}

export default HistoricalArbitrage;
