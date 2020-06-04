import React from 'react';
import { render } from 'react-dom';
import Chart from './StockCharts/Chart';
import CandleStickChartWithMACDIndicator from './StockCharts/CandleStickChartWithMACDIndicator';

import { TypeChooser } from "react-stockcharts/lib/helper";

class ChartComponent extends React.Component {
	
	state={
		data: this.props.data,
		didupdate:false
	}
	
	shouldComponentUpdate(nextProps) {
	  // You don't have to do this check first, but it can help prevent an unneeded render
	  	if (nextProps.data.data !== this.state.data && nextProps.data.data!==undefined) {
			console.log("Went insideComponent Update Called");
	    	var DataCopy =  this.state.date;
	  		DataCopy = nextProps.data.data;
	  		this.setState({ 
	  			data:DataCopy,
	  			didupdate:true
	  		});
	  		return true
	  	}
	  	return false
	}

	render() {
		if (this.state.data == '') {
			return <div>Loading...</div>
		}
		if (this.state.didupdate){
			console.log("Rendering New Chart");
			return (
			<CandleStickChartWithMACDIndicator data={this.state.data} />
			)	
			this.state.didupdate=false;
		}
	}
}

export default ChartComponent;