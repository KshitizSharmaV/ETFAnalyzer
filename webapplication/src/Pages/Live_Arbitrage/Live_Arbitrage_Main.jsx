import React, { useState, useEffect } from 'react';
import Table from 'react-bootstrap/Table';
import Container from 'react-bootstrap/Container';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import axios from 'axios';
import { tsvParse, csvParse } from  "d3-dsv";
import { timeParse } from "d3-time-format";
import Card from 'react-bootstrap/Card'

import ChartComponent from "../../Component/StockPriceChart";
import ScatterPlot from "../../Component/scatterplot";
import AppTable from '../../Component/Table.js';

import LiveStatusWindow from './LiveStatusWindow';

import '../../static/css/Live_Arbitrage.css';

class Live_Arbitrage_Single extends React.Component{
    constructor(props){
        super(props);
    }

    state ={
        Full_Day_Arbitrage_Data: {},
        Full_Day_Prices : '',
        scatterPlotData:'',
        LiveArbitrage:'',
        LiveSpread:'',
        // Prices
        LiveVWPrice:'',
        OpenPrice:'',
        ClosePrice:'',
        HighPrice:'',
        LowPrice:'',
        parseDate : timeParse("%Y-%m-%d %H:%M:%S"),
        CurrentTime:'',
        // Signal
        ETFStatus:'',
        Signal:'',
        SignalStrength:'',
        pnlstatementforday:'',
        LiveColor:'',
    }

    componentDidMount() {
        this.fetchETFLiveData(true);
    }
   
    componentDidUpdate(prevProps,prevState) {
        if (this.props.ETF !== prevProps.ETF) {
            this.fetchETFLiveData(true);
        }
    }

    fetchETFLiveData(){
        // Load the Historical Arbitrgae Data for Today 
        axios.get(`/ETfLiveArbitrage/Single/${this.props.ETF}`).then(res =>{
            console.log("fetchETFLiveData");
            console.log(res);
            this.setState({
                Full_Day_Arbitrage_Data: JSON.parse(res.data.Arbitrage),
                Full_Day_Prices: {'data':tsvParse(res.data.Prices, this.parseData(this.state.parseDate))},
                pnlstatementforday: <AppTable data={JSON.parse(res.data.pnlstatementforday)}/>,
                SignalCategorization: <AppTable data={JSON.parse(res.data.SignalCategorization)}/>,
                scatterPlotData: <ScatterPlot data={JSON.parse(res.data.scatterPlotData)}/>,
            });
        });    
        // Immediately Load the current Live arbitrage Data
        this.UpdateArbitragDataTables(false)

        // Does Iterative calls
        setInterval(() => {
            if ((new Date()).getSeconds() == 8){
                this.UpdateArbitragDataTables(true)
            }
        }, 1000)
    }

    UpdateArbitragDataTables(appendToPreviousTable){
        axios.get(`/ETfLiveArbitrage/Single/UpdateTable/${this.props.ETF}`).then(res =>{
            console.log(res);
            if(appendToPreviousTable){
                console.log("Append To Previous table");
            }else{
                this.setState({
                    LiveArbitrage: res.data.Arbitrage['Arbitrage in $'][0],
                    LiveSpread: res.data.Arbitrage['ETF Trading Spread in $'][0],
                    CurrentTime: res.data.Arbitrage['Time'][0],
                    LiveVWPrice: res.data.Prices['VWPrice'][0],
                    OpenPrice: res.data.Prices['open'][0],
                    ClosePrice: res.data.Prices['close'][0],
                    HighPrice: res.data.Prices['high'][0],
                    LowPrice: res.data.Prices['low'][0],
                    ETFStatus: res.data.SignalInfo.ETFStatus,
                    Signal: res.data.SignalInfo.Signal,
                    SignalStrength: res.data.SignalInfo.Strength,
                    LiveColor: res.data.Arbitrage['Arbitrage in $'][0]<0 ? 'text-success':res.data.Arbitrage['Arbitrage in $'][0]==0? 'text-muted':'text-danger'
                });
            }
        });    
    }

    render(){
        return (
            <Row>
                <Col xs={12} md={4}>
                    <div className="FullPageDiv">
                        <Card>
                          <Card.Header className="text-white" style={{'background-color':'#292b2c'}}>
                              Live Arbitrage
                          </Card.Header>
                          <Card.Body style={{'backgroundColor':'#292b2c'}}>
                            <div className="FullPageDiv">
                                <LiveTable data={this.state.Full_Day_Arbitrage_Data} />
                            </div>
                          </Card.Body>
                        </Card>
                    </div>
                </Col>

                <Col xs={12} md={3}>
                    <Row>
                        
                        <Col xs={12} md={12}>
                        	<LiveStatusWindow HighPrice={this.state.HighPrice} 
                        	OpenPrice={this.state.OpenPrice} 
                        	ClosePrice={this.state.ClosePrice} 
                        	LowPrice={this.state.LowPrice} 
                        	SignalStrength={this.state.SignalStrength} 
                        	CurrentTime={this.state.CurrentTime} 
                        	ETFStatus={this.state.ETFStatus} 
                        	Signal={this.state.Signal}
							LiveArbitrage={this.state.LiveArbitrage}
							LiveSpread={this.state.LiveSpread}
							LiveColor={this.state.LiveColor}
                        	 />
                        </Col>

                        <Col xs={12} md={12}>
                            <Card className="CustomCard">
                                <Card.Header className="CustomCardHeader text-white">
                                    Signal Performace
                                </Card.Header>
                                    <Card.Body className="CustomCardBody text-white">
                                    {this.state.SignalCategorization}
                                  </Card.Body>
                            </Card>
                        </Col>

                        <Col xs={12} md={12}>
                            <Card className="CustomCard">
                                <Card.Header className="CustomCardHeader text-white">
                                    Signal Stats
                                </Card.Header>
                                    <Card.Body className="CustomCardBody text-white">
                                    {this.state.pnlstatementforday}
                                  </Card.Body>
                            </Card>
                        </Col>
                    </Row>
                </Col>

                <Col xs={12} md={5}>
                    <div className="DescriptionTable3">
                        <ChartComponent data={this.state.Full_Day_Prices} />
                    </div>
                    
                    <Row>
                        <Col xs={12} md={7}>
                            <Card className="CustomCard">
                                <Card.Header className="CustomCardHeader text-white">
                                    ETF Change % Vs NAV change %
                                </Card.Header>
                                <Card.Body className="CustomCardBody text-white">
                                    {this.state.scatterPlotData}                            
                                </Card.Body>
                            </Card>
                        </Col>
                    </Row>

                </Col>
            </Row>
        )
    }


    // Parse Data For Stock Price Chart
    parseData(parse) {
        return function(d) {
            d.date = parse(d.date);
            d.open = +parseFloat(d.open);
            d.high = +parseFloat(d.high);
            d.low = +parseFloat(d.low);
            d.close = +parseFloat(d.close);
            d.volume = +parseInt(d.TickVolume);
            
            return d;
        };
    }

}


const TableStyling = {
    fontSize: '13px'
  };

const LiveTable = (props) => {
    if(props.data['Arbitrage in $']==undefined){
        console.log(props.data);
        return "Loading";
    }
    const getKeys = function(someJSON){
        return Object.keys(someJSON);
    }

    const getRowsData = () => {
        var Time = getKeys(props.data.Time)

        return Time.map((key, index) => {
            // console.log(key);
            let cls = "";
            if (props.data['Arbitrage in $'][key] < 0){
                cls = "Red";
            }
            else if(props.data['Arbitrage in $'][key] > 0){
                cls = "Green";
            }
            else {
                cls = "";
            }
            return (
                <tr key={index}>
                    <td className={cls}>{props.data['Time'][key]}</td>
                    <td className={cls}>{props.data['Arbitrage in $'][key]}</td>
                    <td>{props.data['ETF Trading Spread in $'][key]}</td>
                    <td>{props.data['Magnitude of Arbitrage'][key]}</td>
                    <td>{props.data['Over Bought/Sold'][key]}</td>
                    <td>{props.data['Price'][key]}</td>
                    <td>{props.data['ETF Change Price %'][key]}</td>
                    <td>{props.data['TickVolume'][key]}</td>
                </tr>
            )
        })
    }

    return (
        <div className="Table">
          <Table size="sm" striped bordered hover variant="dark"  style={TableStyling}>
          <thead className="TableHead">
            <tr>
                <td>Time</td>
                <td>$Arbitrage</td>
                <td>$Spread</td>
                <td>Absolute Arbitrage</td>
                <td>Over Bought/Sold</td>
                <td>Price</td>
                <td>T</td>
                <td>TickVolume</td>
            </tr>
          </thead>
          <tbody>
            {getRowsData()}
          </tbody>
          </Table>
        </div>          
    );
}

export default Live_Arbitrage_Single;