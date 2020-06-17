import React, { useState, useEffect } from 'react';
import PieChart from './PieChart';
import AppTable from './Table.js';
import '../static/css/Description.css';
import Container from 'react-bootstrap/Container';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import ChartComponent from './StockPriceChart';
import Modal from 'react-bootstrap/Modal'
import Button from 'react-bootstrap/Button';
import axios from 'axios';
import Card from 'react-bootstrap/Card'

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
      parseDate : timeParse("%Y-%m-%d %H:%M:%S")
    }
  }

  componentDidMount() {
    this.fetchETFDescriptionData()
    this.fetchSameIssuer()
    this.fetchSameETFdbCategory()
    }
   
  
  componentDidUpdate(prevProps,prevState) {
      const condition1=this.props.ETF !== prevProps.ETF;
      const condition2=this.props.startDate !== prevProps.startDate;
      
      if (condition1 || condition2) {
        this.fetchETFDescriptionData()
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
    axios.get(`http://localhost:5000/ETfDescription/EtfData/${this.props.ETF}/${this.props.startDate}`).then(res =>{
      console.log(res);
        this.setState({
          DescriptionData : res.data.ETFDataObject,
          HoldingsData : res.data.HoldingsDatObject,
          SimilarTotalAsstUndMgmt: res.data.SimilarTotalAsstUndMgmt,
          IssuerName: res.data.ETFDataObject.Issuer,
          EtfDbCategory: res.data.ETFDataObject.ETFdbCategory
        });
      });
    
    }

  fetchSameIssuer(){
      if(this.state.IssuerName!== null){
        axios.get(`http://localhost:5000/ETfDescription/getETFWithSameIssuer/${this.state.IssuerName}`).then(res =>{
          this.setState({SameIssuerETFs : res.data});
        });
      }
    }


  fetchSameETFdbCategory(){
      if(this.state.EtfDbCategory!== null){
        axios.get(`http://localhost:5000/ETfDescription/getETFsWithSameETFdbCategory/${this.state.EtfDbCategory}`).then(res =>{
            this.setState({EtfsWithSameEtfDbCategory : res.data});
        });
      }
    }

  fetchOHLCDailyData(){
    if(this.state.DescriptionData!== null){
      console.log("Coming in to fetch")
        axios.get(`http://localhost:5000/ETfDescription/getOHLCDailyData/${this.props.ETF}/${this.state.DescriptionData['InceptionDate']}`).then(res =>{
            this.setState({
              OHLCDailyData : {'data':tsvParse(res.data, this.parseData(this.state.parseDate))},
            });
        });
      }
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
        <Container fluid className='pt-3'>
          <Row>
            <Col xs={12} md={9}>
              <Row>
                <Col xs={12} md={4}>
                  <Card>
                    <Card.Header className="text-white BlackHeaderForModal">ETF Description</Card.Header>
                    <Card.Body>
                        <div className="DescriptionTable2">
                          {
                           (this.state.DescriptionData != null) ? <AppTable data={this.state.DescriptionData} clickableTable={'False'} /> : ""
                          }
                        </div>
                    </Card.Body>
                  </Card>
                </Col>
                
                <Col xs={12} md={8}>
                  <Card>
                    <Card.Header className="text-white BlackHeaderForModal">Price Chart</Card.Header>
                    <Card.Body>
                      <ChartComponent data={this.state.OHLCDailyData} />
                    </Card.Body>
                  </Card>
                </Col>
                
                <Col xs={12} md={4}>
                  <Card>
                    <Card.Header className="text-white BlackHeaderForModal">ETFs from same issuer : {this.state.IssuerName}</Card.Header>
                    <Card.Body>
                        <div className="DescriptionTable">
                           <AppTable data={this.state.SameIssuerETFs} clickableTable='True' submitFn={this.props.submitFn}/>
                        </div>
                    </Card.Body>
                  </Card>
                </Col>

                <Col xs={12} md={4}>
                  <Card>
                    <Card.Header className="text-white BlackHeaderForModal">ETF with similar asset under mgmt</Card.Header>
                    <Card.Body>
                        <div className="DescriptionTable">
                           <AppTable data={this.state.SimilarTotalAsstUndMgmt} clickableTable='False' submitFn={this.props.submitFn}/>
                        </div>
                    </Card.Body>
                  </Card>
                </Col>

                <Col xs={12} md={4}>
                <Card>
                    <Card.Header className="text-white BlackHeaderForModal">ETF in same Industry : {this.state.EtfDbCategory}</Card.Header>
                    <Card.Body>
                        <div className="DescriptionTable">
                           <AppTable data={this.state.EtfsWithSameEtfDbCategory} clickableTable='False' submitFn={this.props.submitFn}/>
                        </div>
                    </Card.Body>
                  </Card>
                </Col>

              </Row>
            </Col>
            <Col xs={12} md={3}>
                {
                (this.state.HoldingsData != null) ? <this.HoldingsTableData data={this.state.HoldingsData} clickableTable={'False'} /> : ""
                }
            </Col>
          </Row>
       </Container>
      )
    }


  HoldingsTableData = (props) => {
  const [showPie, setPie] = useState(false);
  const handleClose = () => setPie(false);
  const handleShow = () => setPie(true);
  return (
      <Card>
        <Card.Header className="text-white BlackHeaderForModal">ETF Holdings</Card.Header>
        <Card.Body>
            <PieChart data={props.data} element={"TickerWeight"} />
            <div className="DescriptionTable2">
              <AppTable data={props.data} />
            </div>
        </Card.Body>
      </Card>
    )
  }

}
  

export default Description;