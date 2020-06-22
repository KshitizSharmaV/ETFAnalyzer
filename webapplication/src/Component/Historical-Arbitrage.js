import React from 'react';
import axios from 'axios';
import AppTable from './Table.js';
import Container from 'react-bootstrap/Container'
import Row from 'react-bootstrap/Row'
import Col from 'react-bootstrap/Col'
import Card from 'react-bootstrap/Card'
import StockDesriptionHeader from './StockDesriptionHeader';
import ChartComponent from './StockPriceChart';
import ScatterPlot from './scatterplot';
import PieChartGraph from './PieChart';

import '../static/css/Live_Arbitrage.css';
// Code to display chaer
import { tsvParse, csvParse } from  "d3-dsv";
import { timeParse } from "d3-time-format";


class HistoricalArbitrage extends React.Component{
	constructor(props){
		super(props);
		this.state ={
			etfArbitrageTableData : '',
			historicalArbitrageData:'',
			scatterPlotData:'',
			LoadingStatement: "Loading.. PNL for " + this.props.ETF,
			parseDate : timeParse("%Y-%m-%d %H:%M:%S"),
			etfPriceData:''
		}
		this.fetchDataForADateAndETF = this.fetchDataForADateAndETF.bind(this);
	}

	componentDidMount() {
		this.fetchDataForADateAndETF();
	}
  	
  	// Use instead of unsafe to update
  	componentDidUpdate(prevProps,prevState) {
  		// This updates states which corresponds to any day for any etf
  		// If date changes update the states to get data for the date
  		// If ETFName changes get the data for that day for the etf
  		const condition1=this.props.ETF !== prevProps.ETF;
  		const condition2=this.props.startDate !== prevProps.startDate;
  		if (condition1 || condition2) {
  			this.fetchDataForADateAndETF()
		}

  		// This updates data which is common to an etf - eg all historical PNL dates datas
  		// This data is common for a particular etf. eg for XLK, this data will remain same for all dates
  		// Only update when etfname changes
  		if (condition1) {
  			this.state.LoadingStatement= "Loading.. PNL for " + this.props.ETF;
		}
	}
	
	render(){

  		return(
  			<Row>
	          <Col className="etfArbitrageTable" xs={12} md={5}>
	          	<Card>
				  <Card.Header className="text-white" style={{'background-color':'#292b2c'}}>
					  {this.props.ETF}
					  {this.props.startDate}
				  </Card.Header>
				  <Card.Body style={{'backgroundColor':'#292b2c'}}>
			  		<div className="FullPageDiv">
				    	{this.state.etfArbitrageTableData}
			    	</div>
				  </Card.Body>
				</Card>
	          </Col>

	          <Col xs={12} md={7}>
	          	<Row>
					<Col xs={12} md={8}>
						<Card>
						  <Card.Header className="text-white" style={{'background-color':'#292b2c'}}>Price Chart</Card.Header>
						  <Card.Body style={{'backgroundColor':'#292b2c'}}>
						    <ChartComponent data={this.state.etfPriceData} />
						  </Card.Body>
						</Card>
	          		</Col>

	          		<Col xs={12} md={4}>
						<Card>
							<Card.Header className="text-white" style={{'background-color':'#292b2c'}}>Holdings</Card.Header>
						  <Card.Body style={{'backgroundColor':'#292b2c'}}>
						  	<p>ETF Movers(Weighted)</p>
						    <PieChartGraph data={this.state.etfmoversDictCount} element={"Count"}/>
						    <p>Holdings with most movement</p>
					    	<PieChartGraph data={this.state.highestChangeDictCount} element={"Count"}/>
						  </Card.Body>
						</Card>
	          		</Col>
					
					<Col xs={12} md={6}>
						<h5>PNL Statemens for day</h5>
						{this.state.PNLStatementForTheDay}
					</Col>

					<Col xs={12} md={6}>
						<Card>
							<Card.Header className="text-white" style={{'background-color':'#292b2c'}}>ETF Change % Vs NAV change %</Card.Header>
						  <Card.Body style={{'backgroundColor':'#292b2c'}}>
							{this.state.scatterPlotData}						  	
						  </Card.Body>
						</Card>
					</Col>
				</Row>
	          </Col>
	        </Row>
        )
  	}


  	// Fetch Data For an ETF & a Date
	fetchDataForADateAndETF(url){
		axios.get(`http://localhost:5000/PastArbitrageData/${this.props.ETF}/${this.props.startDate}`).then(res =>{
			console.log(res);
			this.setState({
			 	etfArbitrageTableData : <AppTable data={JSON.parse(res.data.etfhistoricaldata)}/>,
			 	PNLStatementForTheDay : <AppTable data={JSON.parse(res.data.PNLStatementForTheDay)}/>,
			 	etfPriceData : {'data':tsvParse(res.data.etfPrices, this.parseData(this.state.parseDate))},
			 	scatterPlotData: <ScatterPlot data={JSON.parse(res.data.scatterPlotData)}/>,
			 	etfmoversDictCount: JSON.parse(res.data.etfmoversDictCount),
			 	highestChangeDictCount: JSON.parse(res.data.highestChangeDictCount)
			});
			console.log(this.state.etfPriceData);
		});
	}

   	// Parse Data For Stock Price Chart
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

}


export default HistoricalArbitrage;