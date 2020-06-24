import React from "react";
import Axios from "axios";
import { Row, Col, Card } from "react-bootstrap";
import { DescriptionTable } from "./ETF_Descriptions";
import AppTable from "../../Component/Table";
import { ETFHoldings } from "./ETF_Holdings";
import { PNL } from "./PNL";
import { EtfSameIssuer } from "./ETF_Same_Issuer";
import { SimilarAssetUnderManagement } from "./ETF_Similar_Asset_Management";
import { EtfSameIndustry } from "./ETF_Same_Industry";

class ETF_Description extends React.Component {
  state = {
    DescriptionData: null,
    HoldingsData: "",
    SameIssuerETFs: "",
    IssuerName: null,
    SimilarTotalAsstUndMgmt: "",
    EtfsWithSameEtfDbCategory: "",
    EtfDbCategory: null,
    OHLCDailyData: "",
    PNLOverDates: "",
    LoadingStatement: "Loading.. PNL for " + this.props.ETF,
    magnitudeOfArbitrage: "",
  };

  componentDidMount() {
    const { startDate, ETF } = this.props;
    Axios.get(
      `http://localhost:5000/ETfDescription/EtfData/${ETF}/${startDate}`
    )
      .then((res) => {
        const { ETFDataObject, SimilarTotalAsstUndMgmt } = res.data;
        this.setState({
          ...this.state,
          DescriptionData: ETFDataObject,
          SimilarTotalAsstUndMgmt: SimilarTotalAsstUndMgmt,
          IssuerName: ETFDataObject.Issuer,
          EtfDbCategory: ETFDataObject.ETFdbCategory,
        });
      })
      .catch((err) => console.log(err));
  }

  render() {
      console.log(this.state)
    const {
      DescriptionData,
      IssuerName,
      SimilarTotalAsstUndMgmt,
      EtfDbCategory,
    } = this.state;
    const { startDate, ETF } = this.props;
    return (
      <Row>
        <Col xs={12} md={12}>
          <Row>
            <Col xs={12} md={3}>
              {DescriptionData && (
                <DescriptionTable DescriptionData={DescriptionData} />
              )}
            </Col>

            {/*
        <Col xs={12} md={8}>
          <Card>
            <Card.Header className="text-white BlackHeaderForModal">Price Chart</Card.Header>
            <Card.Body style={{'backgroundColor':'#292b2c'}}>
              <ChartComponent data={this.state.OHLCDailyData} />
            </Card.Body>
          </Card>
        </Col>
      */}

            <Col xs={12} md={3}>
              <ETFHoldings ETF={ETF} startDate={startDate} />
            </Col>

            <Col xs={12} md={6}>
              <PNL ETF={ETF} />
            </Col>

            <Col xs={12} md={4}>
              <EtfSameIssuer IssuerName={IssuerName} />
            </Col>

            <Col xs={12} md={4}>
              {SimilarTotalAsstUndMgmt && (
                <SimilarAssetUnderManagement
                  SimilarTotalAsstUndMgmt={SimilarTotalAsstUndMgmt}
                />
              )}
            </Col>

            <Col xs={12} md={4}>
              {EtfDbCategory && (
                <EtfSameIndustry EtfDbCategory={EtfDbCategory} />
              )}
            </Col>
          </Row>
        </Col>
      </Row>
    );
  }
}

export default ETF_Description;
