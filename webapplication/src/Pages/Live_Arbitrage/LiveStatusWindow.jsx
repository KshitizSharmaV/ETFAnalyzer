import React from "react";
import Card from 'react-bootstrap/Card';
import '../../static/css/Live_Arbitrage.css';

const LiveStatusWindow = (props) => {
  return (
    <Card className="CustomCard">
        <Card.Header className="CustomCardHeader text-white">
            <span className="h4 pull-left pr-2"></span>
            H: <span className="text-muted">{props.HighPrice} </span>
            O: <span className="text-muted">{props.OpenPrice} </span>
            C: <span className="text-muted">{props.ClosePrice} </span>
            L: <span className="text-muted">{props.LowPrice} </span>

            <div>Time: <span className="text-muted">{props.CurrentTime}</span></div>
        </Card.Header>
    
          <Card.Body className="CustomCardBody text-white">
            <div><h5><span className={props.LiveColor}>ETF Status: {props.ETFStatus}</span></h5></div>
            <div><h5><span className={props.LiveColor}>Signal: {props.Signal}</span></h5></div>
            <div><span className={props.LiveColor}>Strength: {props.SignalStrength}</span></div>

            <div><span className="">$ Arbitrage: {props.LiveArbitrage}</span></div>
            <div><span className="">$ Spread: {props.LiveSpread}</span></div>    
          </Card.Body>
    </Card>
  );
};

export default LiveStatusWindow;
