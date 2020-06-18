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
        LiveData: {},
    }

    componentDidMount() {
        this.fetchETFLiveData()
    }
  
    fetchETFLiveData(){
        this.updateData();
        setInterval(() => {
            if ((new Date()).getSeconds() == 8){
                this.updateData();
            }
        }, 1000)
    }


    updateData(){
        axios.get(`http://localhost:5000/ETfLiveArbitrage/AllTickers`).then(res =>{
            console.log(res);
            this.setState({
                Arbitrage: res.data['Arbitrage in $'],
                Spread: res.data['ETF Trading Spread in $'],
                Symbol: res.data.Symbol,
                VWPrice: res.data.VWPrice,
                TickVolume: res.data.TickVolume,
                NAVChange_pct: res.data['Net Asset Value Change%'],
                ETFPriceChange_pct: res.data['ETF Change Price %'],
                ETFMover1: res.data['ETFMover%1'],
                ETFMover2: res.data['ETFMover%2'],
                Change1: res.data['Change%1'],
                Change2: res.data['Change%2'],
                
                time: (new Date()).toLocaleString(),
            });
        });
    }

    render(){
        return (
            <Container fluid>
                <Row>
                    <Col xs={12} md={12}>
                    <p>{this.state.time}</p>
                        <div className="LiveArbitrageTable pt-1">
                            {
                                (this.state.Symbol != null) ? <LiveTable data={this.state} /> : ""
                            }
                        </div>
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
                    <td>{props.data.NAVChange_pct[key]}</td>
                    <td>{props.data.ETFPriceChange_pct[key]}</td>
                    <td>{props.data.VWPrice[key]}</td>
                    <td>{props.data.TickVolume[key]}</td>
                    <td>{props.data.ETFMover1[key][0]}</td>
                    <td>{props.data.Change1[key][0]}</td>
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
                <td>$ Arbitrage</td>
                <td>$ Spread</td>
                <td>NAV Change %</td>
                <td>ETF Change %</td>
                <td>$ Price</td>
                <td>Tick Volume</td>
                <td>ETFMover</td>
                <td>H Change</td>
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