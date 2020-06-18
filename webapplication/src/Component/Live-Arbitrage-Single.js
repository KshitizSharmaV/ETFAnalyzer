import React, { useState, useEffect } from 'react';
import AppTable from './Table.js';
import Table from 'react-bootstrap/Table';
import '../static/css/Live_Arbitrage.css';
import Container from 'react-bootstrap/Container';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import axios from 'axios';
import { tsvParse, csvParse } from  "d3-dsv";
import { timeParse } from "d3-time-format";
import Card from 'react-bootstrap/Card'
import ScatterPlot from './scatterplot';
import ChartComponent from './StockPriceChart';


class Live_Arbitrage extends React.Component{
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
        axios.get(`http://localhost:5000/ETfLiveArbitrage/Single/${this.props.ETF}`).then(res =>{
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
        axios.get(`http://localhost:5000/ETfLiveArbitrage/Single/UpdateTable/${this.props.ETF}`).then(res =>{
            console.log(res);
            if(appendToPreviousTable){
                console.log("Append To Previous table");
            }else{
                this.setState({
                    LiveArbitrage: res.data.Arbitrage.Arbitrage[0],
                    LiveSpread: res.data.Arbitrage.Spread[0],
                    CurrentTime: res.data.Arbitrage.Timestamp[0],
                    LiveVWPrice: res.data.Prices.VWPrice[0],
                    OpenPrice: res.data.Prices.open[0],
                    ClosePrice: res.data.Prices.close[0],
                    HighPrice: res.data.Prices.high[0],
                    LowPrice: res.data.Prices.low[0],
                    ETFStatus: res.data.SignalInfo.ETFStatus,
                    Signal: res.data.SignalInfo.Signal,
                    SignalStrength: res.data.SignalInfo.Strength,
                    LiveColor: res.data.Arbitrage.Arbitrage[0]<0 ? 'text-success':res.data.Arbitrage.Arbitrage[0]==0? 'text-muted':'text-danger'
                });
            }
        });    
    }

    render(){
        return (
            <Container fluid>
            <Row className="mt-1">
                <Col xs={12} md={4}>
                    <div className="LiveArbitrageTable">
                        <LiveTable data={this.state.Full_Day_Arbitrage_Data} />
                    </div>
                </Col>

                <Col xs={12} md={3}>
                    <div className="">
                        <Card>
                            <Card.Header className="text-white" style={{'backgroundColor':'#292b2c'}}>
                                <span className="h4 pull-left pr-2">{this.props.ETF}</span>
                                H: <span className="text-muted">{this.state.HighPrice} </span>
                                O: <span className="text-muted">{this.state.OpenPrice} </span>
                                C: <span className="text-muted">{this.state.ClosePrice} </span>
                                L: <span className="text-muted">{this.state.LowPrice} </span>

                                <div>Time: <span className="text-muted">{this.state.CurrentTime}</span></div>
                            </Card.Header>
                        
                              <Card.Body>
                                <div><h5><span className={this.state.LiveColor}>ETF Status: {this.state.ETFStatus}</span></h5></div>
                                <div><h5><span className={this.state.LiveColor}>Signal: {this.state.Signal}</span></h5></div>
                                <div><span className={this.state.LiveColor}>Strength: {this.state.SignalStrength}</span></div>

                                <div><span className="">$ Arbitrage: {this.state.LiveArbitrage}</span></div>
                                <div><span className="">$ Spread: {this.state.LiveSpread}</span></div>    
                              </Card.Body>
                        </Card>
                    </div>

                    <div className="pt-3">
                        {this.state.pnlstatementforday}
                    </div>

                    <div className="pt-3">
                        {this.state.SignalCategorization}
                    </div>

                </Col>

                <Col xs={12} md={5}>
                    <div className="DescriptionTable3">
                        <ChartComponent data={this.state.Full_Day_Prices} />
                    </div>
                    
                    <Row>
                        <Col xs={12} md={6}>
                            {this.state.scatterPlotData}                            
                        </Col>
                    </Row>

                </Col>
            </Row>
        </Container>
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
    if(props.data.Arbitrage==undefined){
        console.log(props.data);
        return "Loading";
    }
    const getKeys = function(someJSON){
        return Object.keys(someJSON);
    }

    const getRowsData = () => {
        var Symbols = getKeys(props.data.Symbol)

        return Symbols.map((key, index) => {
            // console.log(key);
            let cls = "";
            if (props.data.Arbitrage[key] < 0){
                cls = "Red";
            }
            else if(props.data.Arbitrage[key] > 0){
                cls = "Green";
            }
            else {
                cls = "";
            }
            return (
                <tr key={index}>
                    <td className={cls}>{props.data.Timestamp[key]}</td>
                    <td className={cls}>{props.data.Arbitrage[key]}</td>
                    <td>{props.data.Spread[key]}</td>
                    <td>{props.data.VWPrice[key]}</td>
                    <td>{props.data.TickVolume[key]}</td>
                </tr>
            )
        })
    }

    return (
        <div className="Table">
          <Table striped bordered hover variant="dark"  style={TableStyling}>
          <thead className="TableHead">
            <tr>
                <td>Time</td>
                <td>Arbitrage</td>
                <td>Spread</td>
                <td>Price</td>
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

export default Live_Arbitrage;