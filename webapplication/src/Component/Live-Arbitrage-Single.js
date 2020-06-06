import React, { useState, useEffect } from 'react';
import AppTable from './Table.js';
import Table from 'react-bootstrap/Table';
import '../static/css/Description.css';
import Container from 'react-bootstrap/Container';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import axios from 'axios';
import { tsvParse, csvParse } from  "d3-dsv";
import { timeParse } from "d3-time-format";

import ChartComponent from './StockPriceChart';


class Live_Arbitrage extends React.Component{
    constructor(props){
        super(props);
    }

    state ={
        Full_Day_Arbitrage_Data: {},
        Full_Day_Prices : '',
        LivePrice:'',
        LiveArbitrage:'',
        LiveSpread:'',
        LivePrice:'',
        parseDate : timeParse("%Y-%m-%d %H:%M:%S"),
        CurrentTime:''
    }

    componentDidMount() {
        this.fetchETFLiveData(true);
    }
   
    componentDidUpdate(prevProps,prevState) {
        if (this.props.ETF !== prevProps.ETF) {
            this.fetchETFLiveData(true);
        }
    }

    fetchETFLiveData(newEtfWasRequested){
        this.UpdateArbitragDataTables(newEtfWasRequested)
        setInterval(() => {
            if ((new Date()).getSeconds() == 13){
                this.UpdateArbitragDataTables(false)
            }
        }, 1000)
    }

    UpdateArbitragDataTables(newEtfWasRequested){
        console.log(newEtfWasRequested);
        if(newEtfWasRequested){
            axios.get(`http://localhost:5000/ETfLiveArbitrage/Single/${this.props.ETF}`).then(res =>{
                console.log(res);
                this.setState({
                    Full_Day_Arbitrage_Data: res.data.Full_Day_Arbitrage_Data,
                    Full_Day_Prices: {'data':tsvParse(res.data.Full_Day_Prices, this.parseData(this.state.parseDate))}
                });
                console.log(this.state.Full_Day_Prices);
            });    
        }else{
            axios.get(`http://localhost:5000/ETfLiveArbitrage/Single/UpdateTable/${this.props.ETF}`).then(res =>{
                console.log(res);
                this.setState({
                    LiveArbitrage: res.data.LiveArbitrage.Arbitrage[0],
                    LiveSpread: res.data.LiveArbitrage.Spread[0],
                    Price: res.data.LiveArbitrage.Price[0],
                    CurrentTime: res.data.LiveArbitrage.Timestamp[0],
                });
                console.log(this.state.FullDay);
            });    
        }
        
    }

    render(){
        return (
            <Container fluid>
            <h4> Live Arbitrage </h4>
            <h5> {this.props.ETF} </h5>
            <br />
            <Row>
                <Col xs={12} md={3}>
                    <div className="DescriptionTable3">
                        <LiveTable data={this.state.Full_Day_Arbitrage_Data} />
                    </div>
                </Col>

                <Col xs={12} md={3}>
                    <div className="DescriptionTable3">
                        <p>{this.state.CurrentTime}</p>
                        <p>{this.state.LiveArbitrage}</p>
                        <p>{this.state.LiveSpread}</p>
                        <p>{this.state.Price}</p>
                    </div>
                </Col>

                <Col xs={12} md={6}>
                    <div className="DescriptionTable3">
                        <ChartComponent data={this.state.Full_Day_Prices} />
                    </div>
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