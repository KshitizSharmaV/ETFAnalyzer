import React, { useState, useEffect } from 'react';
import PieChart from './PieChart';
import AppTable from './Table.js';
import '../static/css/Description.css';
import '../static/css/style.css';
import Container from 'react-bootstrap/Container';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import ChartComponent from './StockPriceChart';
import Modal from 'react-bootstrap/Modal'
import Button from 'react-bootstrap/Button';
import axios from 'axios';
import Card from 'react-bootstrap/Card'
import { Dropdown,DropdownButton,ButtonGroup } from 'react-bootstrap'

import { tsvParse, csvParse } from  "d3-dsv";
import { timeParse } from "d3-time-format";

class Description extends React.Component{
  
  constructor(props){
    super(props);
    this.state ={
      DescriptionData :null,
      HoldingsData :'',
      SameIssuerETFs:'',
      IssuerName:null,
      SimilarTotalAsstUndMgmt:'',
      EtfsWithSameEtfDbCategory:'',
      EtfDbCategory:null,
      OHLCDailyData:'',
      PNLOverDates:'',
      LoadingStatement: "Loading.. PNL for " + this.props.ETF,
      magnitudeOfArbitrage:'',
      parseDate : timeParse("%Y-%m-%d %H:%M:%S")
    }
  }

  componentDidMount() {
    this.fetchETFDescriptionData()
    this.fetchSameIssuer()
    this.fetchSameETFdbCategory()
    this.fetchHoldingsData()
    this.fetchDataCommonToAllDates()
    }
   
  
  componentDidUpdate(prevProps,prevState) {
      const condition1=this.props.ETF !== prevProps.ETF;
      const condition2=this.props.startDate !== prevProps.startDate;
      
      if (condition1 || condition2) {
        this.fetchETFDescriptionData();
        this.fetchHoldingsData()
      }

      if (this.state.IssuerName !== prevState.IssuerName){
        this.fetchSameIssuer();
      }

      if (this.state.EtfDbCategory !== prevState.EtfDbCategory){
        this.fetchSameETFdbCategory();
      }

      if(this.state.DescriptionData!==prevState.DescriptionData){
          this.fetchOHLCDailyData();
      }
    }

  
  fetchETFDescriptionData(){
    axios.get(`/ETfDescription/EtfData/${this.props.ETF}/${this.props.startDate}`).then(res =>{
      console.log(res);
        this.setState({
          DescriptionData : res.data.ETFDataObject,
          SimilarTotalAsstUndMgmt: res.data.SimilarTotalAsstUndMgmt,
          IssuerName: res.data.ETFDataObject.Issuer,
          EtfDbCategory: res.data.ETFDataObject.ETFdbCategory
        });
      });
    
    }

  fetchSameIssuer(){
      if(this.state.IssuerName!== null){
        axios.get(`/ETfDescription/getETFWithSameIssuer/${this.state.IssuerName}`).then(res =>{
          this.setState({SameIssuerETFs : res.data});
        });
      }
    }

    fetchHoldingsData(){
        axios.get(`/ETfDescription/getHoldingsData/${this.props.ETF}/${this.props.startDate}`).then(res =>{
          console.log(res);
          this.setState({HoldingsData : res.data});
        });
    }


  fetchSameETFdbCategory(){
      if(this.state.EtfDbCategory!== null){
        axios.get(`/ETfDescription/getETFsWithSameETFdbCategory/${this.state.EtfDbCategory}`).then(res =>{
            this.setState({EtfsWithSameEtfDbCategory : res.data});
        });
      }
    }

  fetchOHLCDailyData(){
    if(this.state.DescriptionData!== null){
      console.log("Coming in to fetch")
        axios.get(`/ETfDescription/getOHLCDailyData/${this.props.ETF}/${this.state.DescriptionData['InceptionDate']}`).then(res =>{
            this.setState({
              OHLCDailyData : {'data':tsvParse(res.data, this.parseData(this.state.parseDate))},
            });
        });
      }
  }
  
  // Fetch Data which is common to an ETF across all dates
  fetchDataCommonToAllDates(url){
    axios.get(`/PastArbitrageData/CommonDataAcrossEtf/${this.props.ETF}`).then(res =>{
    this.setState({
      PNLOverDates: <AppTable data={JSON.parse(res.data.PNLOverDates)}/>,
      magnitudeOfArbitrage : 0
      });
    });
  }

  parseData(parse) {
    return function(d) {
      d.date = parse(d.date);
      d.open = +parseFloat(d.open);
      d.high = +parseFloat(d.high);
      d.low = +parseFloat(d.low);
      d.close = +parseFloat(d.close);
      d.volume = +parseInt(d.volume);
      
      return d;
    };
  }  

  render(){
      return (
          <Row>
            <Col xs={12} md={12}>
              <Row>
                <Col xs={12} md={3}>
                  <Card>
                    <Card.Header className="text-white BlackHeaderForModal">ETF Description</Card.Header>
                    <Card.Body className="CustomBackGroundColor">
                        <div className="DescriptionTable2">
                          {
                           (this.state.DescriptionData != null) ? <AppTable data={this.state.DescriptionData} clickableTable={'False'} /> : ""
                          }
                        </div>
                    </Card.Body>
                  </Card>
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
                {
                (this.state.HoldingsData != null) ? <this.HoldingsTableData data={this.state.HoldingsData} clickableTable={'False'} /> : ""
                }
              </Col>

              <Col xs={12} md={6}>
                  <Card>
                    <Card.Header className="text-white BlackHeaderForModal">
                      <div class="row">
                        <div class="col-md-8">
                          PNL Data
                        </div>
                        <div class="col-md-4 float-right">
                          Arbitrage Magnitude : {this.MagnitudeOfArbitrageScorllable()}
                        </div>
                      </div>
                    </Card.Header>
                    <Card.Body className="CustomBackGroundColor">
                      <div className="DescriptionTable2">
                        {
                          (this.state.PNLOverDates) ? this.state.PNLOverDates : this.state.LoadingStatement
                        }
                      </div>
                    </Card.Body>
                  </Card>
                </Col>
                
                <Col xs={12} md={4}>
                  <Card className="mb-2">
                    <Card.Header className="text-white BlackHeaderForModal">ETFs from same issuer : {this.state.IssuerName}</Card.Header>
                    <Card.Body className="CustomBackGroundColor">
                        <div className="DescriptionTable">
                           <AppTable data={this.state.SameIssuerETFs} clickableTable='False' submitFn={this.props.submitFn}/>
                        </div>
                    </Card.Body>
                  </Card>
                </Col>

                <Col xs={12} md={4}>
                  <Card className="mb-2">
                    <Card.Header className="text-white BlackHeaderForModal">ETF with similar asset under mgmt</Card.Header>
                    <Card.Body className="CustomBackGroundColor">
                        <div className="DescriptionTable">
                           <AppTable data={this.state.SimilarTotalAsstUndMgmt} clickableTable='False' submitFn={this.props.submitFn}/>
                        </div>
                    </Card.Body>
                  </Card>
                </Col>

                <Col xs={12} md={4}>
                <Card className="mb-2">
                    <Card.Header className="text-white BlackHeaderForModal">ETF in same Industry : {this.state.EtfDbCategory}</Card.Header>
                    <Card.Body className="CustomBackGroundColor">
                        <div className="DescriptionTable">
                           <AppTable data={this.state.EtfsWithSameEtfDbCategory} clickableTable='False' submitFn={this.props.submitFn}/>
                        </div>
                    </Card.Body>
                  </Card>
                </Col>

              </Row>
            </Col>
          </Row>
      )
    }


  HoldingsTableData = (props) => {
  const [showPie, setPie] = useState(false);
  const handleClose = () => setPie(false);
  const handleShow = () => setPie(true);
  return (
      <Card>
        <Card.Header className="text-white BlackHeaderForModal">ETF Holdings</Card.Header>
        <Card.Body className="CustomBackGroundColor">
            {/* Pie Chart Commented
            <PieChart data={props.data} element={"TickerWeight"} />
            */}
            <div className="DescriptionTable2">
              <AppTable data={props.data} />
            </div>
        </Card.Body>
      </Card>
    )
  }

  MagnitudeOfArbitrageScorllable = () =>{
    return(
       <DropdownButton
        as={ButtonGroup}
        key={1}
        variant={"Arbitrage"}
        className="Warning"
        title={"0.0"}
        size="sm"
      >
        <Dropdown.Item eventKey="0">0.0</Dropdown.Item>
        <Dropdown.Item eventKey="1">0.01</Dropdown.Item>
        <Dropdown.Item eventKey="2">0.02</Dropdown.Item>
        <Dropdown.Item eventKey="3">0.03</Dropdown.Item>
        <Dropdown.Item eventKey="4">0.04</Dropdown.Item>
        <Dropdown.Item eventKey="5">0.05</Dropdown.Item>
        <Dropdown.Item eventKey="6">0.06</Dropdown.Item>
        <Dropdown.Item eventKey="7">0.07</Dropdown.Item>
        <Dropdown.Item eventKey="8">0.08</Dropdown.Item>
        <Dropdown.Item eventKey="9">0.09</Dropdown.Item>
        <Dropdown.Item eventKey="10">0.10</Dropdown.Item>
        <Dropdown.Item eventKey="11">0.11</Dropdown.Item>
        <Dropdown.Item eventKey="12">0.12</Dropdown.Item>
        <Dropdown.Item eventKey="13">0.13</Dropdown.Item>
        <Dropdown.Item eventKey="14">0.14</Dropdown.Item>
        <Dropdown.Item eventKey="15">0.15</Dropdown.Item>
      </DropdownButton>
    )
  }


}
  

export default Description;