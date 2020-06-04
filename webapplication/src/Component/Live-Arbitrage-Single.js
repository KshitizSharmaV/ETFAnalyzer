import React, { useState, useEffect } from 'react';
import AppTable from './Table.js';
import Table from 'react-bootstrap/Table';
import '../static/css/Description.css';
import Container from 'react-bootstrap/Container';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import axios from 'axios';



class Live_Arbitrage extends React.Component{
    constructor(props){
        super(props);
    }

    state ={
        LiveData: {},
        FullDay : {},
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
                    LiveData: res.data.Live,
                    time: (new Date(res.data.Full_Day.Timestamp["0"])).toLocaleString(),
                    FullDay: res.data.Full_Day,
                });
                console.log(this.state.FullDay);
            });    
        }else{
            axios.get(`http://localhost:5000/ETfLiveArbitrage/Single/UpdateTable/${this.props.ETF}`).then(res =>{
                console.log(res);
                this.setState({
                    LiveData: res.data.Live,
                    time: (new Date(res.data.Full_Day.Timestamp["0"])).toLocaleString(),
                    FullDay: res.data.Full_Day,
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
            <h5> {this.state.time} </h5>
            <br />
            <Row>
                <Col xs={12} md={6}>
                    <div className="DescriptionTable3">
                        {
                            (this.state.LiveData) ? <AppTable data={this.state.LiveData} /> : "Loadingg"
                        }
                    </div>
                </Col>
                <Col xs={12} md={6}>
                    <div className="DescriptionTable3">
                        <LiveTable data={this.state.FullDay} />
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
    //console.log(props.data);
    if(props.data.Symbol == null){
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
                    <td className={cls}>{new Date(props.data.Timestamp[key]).toLocaleTimeString()}</td>
                    <td className={cls}>{props.data.Arbitrage[key]}</td>
                    <td>{props.data.Spread[key]}</td>
                    <td>{props.data.Price[key]}</td>
                    <td>{props.data.Symbol[key]}</td>
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
                <td>Volume</td>
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