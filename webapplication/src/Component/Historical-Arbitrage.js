import React,{ useState, useEffect } from 'react';
import axios from 'axios';
import AppTable from './Table.js';
import Container from 'react-bootstrap/Container'
import Row from 'react-bootstrap/Row'
import Button from 'react-bootstrap/Button'
import Col from 'react-bootstrap/Col'
import Card from 'react-bootstrap/Card'
import StockDesriptionHeader from './StockDesriptionHeader';
import ChartComponent from './StockPriceChart';
import ScatterPlot from './scatterplot';
import PieChartGraph from './PieChart';
import LineChartForHistArbJs from './LineChartForHistArb';
import Modal from 'react-bootstrap/Modal'
import Table from "react-bootstrap/Table";

import '../static/css/Live_Arbitrage.css';
import '../static/css/style.css';
// Code to display chaer
import { tsvParse, csvParse } from  "d3-dsv";
import { timeParse } from "d3-time-format";


class HistoricalArbitrage extends React.Component{
	constructor(props){
		super(props);
		this.state ={
			etfArbitrageTableData : null,
			historicalArbitrageData:'',
			scatterPlotData:'',
			LoadingStatement: "Loading.. PNL for " + this.props.ETF,
			parseDate : timeParse("%Y-%m-%d %H:%M:%S"),
			etfPriceData:'',
			ArbitrageCumSum:''
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
				    <Card.Header className="modalCustomHeader text-white CustomBackGroundColor">
					  <div className="row">
                        <div className="col-md-10">
                          {this.props.ETF} {this.props.startDate}
                        </div>
                        <div className="col-md-2 float-right">
                        <CombinedPieCharts etfmoversDictCount={this.state.etfmoversDictCount} highestChangeDictCount={this.state.highestChangeDictCount}/>
                        </div>
                      </div>
					</Card.Header>
				  <Card.Body className="BlackHeaderForModal">
			  		<div className="FullPageDiv">
				    	<EtfArbitrageTable_Func data={this.state.etfArbitrageTableData} />
				    </div>
				  </Card.Body>
				</Card>
	          </Col>

	          <Col xs={12} md={7}>
	          	
	          	<Row>
	          		<Col xs={12} md={4}>
						
						<Row>
							<Col xs={12} md={12}>
								<Card className="CustomCard">
									<Card.Header className="CustomCardHeader text-white">
									PNL</Card.Header>
								  	<Card.Body className="CustomCardBody text-white">
										{this.state.PNLStatementForTheDay}
								  	</Card.Body>
								</Card>
							</Col>

							<Col xs={12} md={12}>

								<Card className="CustomCard">
									<Card.Header className="CustomCardHeader text-white">
										Arb Time Series
									</Card.Header>
								  	<Card.Body className="CustomCardBody text-white">
										<LineChartForHistArbJs data={this.state.ArbitrageCumSum}/>
								  	</Card.Body>
								</Card>
							</Col>
						</Row>
	          		</Col>

	          		<Col xs={12} md={8}>

						<Row>
							<Col xs={12} md={12}>
								<Card>
								  <Card.Header className="modalCustomHeader text-white CustomBackGroundColor">Price Chart</Card.Header>
								  <Card.Body className="BlackHeaderForModal">
								    <ChartComponent data={this.state.etfPriceData} />
								  </Card.Body>
								</Card>
							</Col>
							<Col xs={12} md={12}>
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

	          </Col>
	        </Row>
        )
  	}


  	// Fetch Data For an ETF & a Date
	fetchDataForADateAndETF(url){
		axios.get(`/PastArbitrageData/${this.props.ETF}/${this.props.startDate}`).then(res =>{
			console.log(res);
			this.setState({
			 	etfArbitrageTableData : JSON.parse(res.data.etfhistoricaldata),
			 	PNLStatementForTheDay : <AppTable data={JSON.parse(res.data.PNLStatementForTheDay)}/>,
			 	etfPriceData : {'data':tsvParse(res.data.etfPrices, this.parseData(this.state.parseDate))},
			 	scatterPlotData: <ScatterPlot data={JSON.parse(res.data.scatterPlotData)}/>,
			 	etfmoversDictCount: JSON.parse(res.data.etfmoversDictCount),
			 	highestChangeDictCount: JSON.parse(res.data.highestChangeDictCount),
			 	ArbitrageCumSum : res.data.ArbitrageCumSum
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


// Pie Chart whic is the modal windo
const CombinedPieCharts = (props) =>{
	  const [show, setShow] = useState(false);

	  const handleClose = () => setShow(false);
	  const handleShow = () => setShow(true);
	  console.log(props);
	  return (
	   <>
      <Button variant="warning" size="sm" onClick={handleShow}>
        Etf Movers
      </Button>

      <Modal dialogClassName="my-modal" show={show} onHide={handleClose}>
        <Modal.Header closeButton>
          <Modal.Title>
          	Underlying Holdings
          </Modal.Title>
        </Modal.Header>
        <Modal.Body>
        	<Row>
          		<Col xs={12} md={12}>
					<Card>
						<Card.Header className="CustomCardHeader text-white CustomBackGroundColor">Holdings</Card.Header>
						<Card.Body className="CustomCardBody text-white">
							<Row>
								<Col xs={12} md={6}>
									<center><h5>Etf Movers</h5></center>
									<PieChartGraph data={props.etfmoversDictCount} element={"Count"}/>
								</Col>
								<Col xs={12} md={6}>
									<center><h5>Market Movers</h5></center>
									<PieChartGraph data={props.highestChangeDictCount} element={"Count"}/>
								</Col>
							</Row>
						</Card.Body>
					</Card>
				</Col>
			</Row>
        </Modal.Body>
        <Modal.Footer>
          <Button variant="secondary" onClick={handleClose}>
            Close
          </Button>
          <Button variant="primary" onClick={handleClose}>
            Save Changes
          </Button>
        </Modal.Footer>
      </Modal>
    </>
  );
}


const TableStyling = {
    fontSize: '13px'
  };

const EtfArbitrageTable_Func = (props) =>{
	
	if(props.data == {} || props.data == undefined){
        return "Loading";
    }

	const { data } = props;
   	
   	const getKeys = function(someJSON){
        return Object.keys(someJSON);
    }

   const getRowsData = () => {
   		var Time = getKeys(props.data.Time)

        return Time.map((key, index) => {
            //console.log(key);
            let cls = "";
            if (props.data['Over Bought/Sold'][key] == 'Over Bought'){
                cls = "Green";
            }
            else if(props.data['Over Bought/Sold'][key] == 'Over Sold'){
                cls = "Red";
            }
            else {
                cls = "";
            }
            return (
                <tr key={index}>
                    <td className={cls}>{props.data['Time'][key]}</td>
                    <td className={cls}>{props.data['$Arbitrage'][key]}</td>
                    <td className={cls}>{props.data['$Spread'][key]}</td>
                    <td className={cls}>{props.data['Absolute Arbitrage'][key]}</td>
                    <td className={cls}>{props.data['Over Bought/Sold'][key]}</td>
                    <td>{props.data['Etf Mover'][key]}</td>
                    <td>{props.data['Most Change%'][key]}</td>
                    <td>{props.data['T'][key]}</td>
                    <td>{props.data['T+1'][key]}</td>
                </tr>
            )
        })
    }

   return (
     	   <Table size="sm" striped bordered hover variant="dark" style={TableStyling}>
             <thead>
               <tr>
                 <th>Time</th>
                 <th>$Arbitrage</th>
                 <th>$Spread</th>
                 <th>Absolute Arbitrage</th>
                 <th>Over Bought/Sold</th>
                 <th>Etf Mover</th>
                 <th>Most Change%</th>
                 <th>T</th>
                 <th>T+1</th>
               </tr>
             </thead>
             <tbody>
               {getRowsData()}
             </tbody>
           </Table>
   );

}






export default HistoricalArbitrage;