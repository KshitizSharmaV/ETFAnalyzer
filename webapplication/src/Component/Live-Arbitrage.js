import React, { useState, useEffect } from 'react';
import Table from 'react-bootstrap/Table'
import '../static/css/Live_Arbitrage.css';
import Container from 'react-bootstrap/Container';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import axios from 'axios';
import { ChartCanvas, Chart } from "react-stockcharts";
import PropTypes from "prop-types";

import '../static/css/Live_Arbitrage.css';


class Live_Arbitrage extends React.Component{
    constructor(props){
        super(props);
    }

    state = {
        seconds: (new Date()).getSeconds(),
        LiveData: {},
    }

    componentDidMount() {
        axios.get(`http://localhost:5000/ETfLiveArbitrage/AllTickers`).then(res =>{
            console.log(res);
            this.setState({
                Arbitrage: res.data.Arbitrage,
                Spread: res.data.Spread,
                Symbol: res.data.Symbol,
                VWPrice: res.data.VWPrice,
                TickVolume: res.data.TickVolume,
                time: (new Date()).toLocaleString(),
            });
            // console.log(this.state);
        });
        console.log(this.state.seconds);
        this.fetchETFLiveData()
    }
  
    fetchETFLiveData(){
        setInterval(() => {
            this.setState({
                seconds : (this.state.seconds > 59) ? 1 : this.state.seconds + 1
            });
            console.log(this.state.seconds);
            if (this.state.seconds == 13){
                console.log("this is something")
                axios.get(`http://localhost:5000/ETfLiveArbitrage/AllTickers`).then(res =>{
                    this.setState({
                        Arbitrage: res.data.Arbitrage,
                        Spread: res.data.Spread,
                        Symbol: res.data.Symbol,
                        VWPrice: res.data.VWPrice,
                        TickVolume: res.data.TickVolume,
                        time: (new Date()).toLocaleString(),
                    });
                    console.log(this.state);
                });
            }
        }, 1000)
    }

    render(){
        return (
            <Container fluid>
                <Row>
                    <Col xs={12} md={6}>
                        <div className="LiveArbitrageTable pt-1">
                            {
                                (this.state.Symbol != null) ? <LiveTable data={this.state} /> : ""
                            }
                        </div>
                    </Col>
                    <Col xs={12} md={6}>
                        <p>{this.state.time}</p>
                    </Col>
                </Row>
            </Container>
        )
    }
}


const TableStyling = {
    fontSize: '13px'
  };

const LiveTable = (props) => {
    if(props.data == {} || props.data == undefined){
        return "Loading";
    }
    console.log(props);
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
                    <td className={cls}>{props.data.Symbol[key]}</td>
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
                <td>Symbol</td>
                <td>Arbitrage</td>
                <td>Spread</td>
                <td>Price</td>
                <td>Tick Volume</td>
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