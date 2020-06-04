import React, {Component} from 'react';


class StockDesriptionHeader extends Component {
	
	render(){
		return (
		<div>
			<h5> {this.props.ETF} </h5>
    		<h5> Date of Arbitrage : {this.props.startDate}</h5>
    		<ColoredLine color="black" />
		</div>
		);
	}
}


const ColoredLine = ({ color }) => (
<hr
    style={{
        color: color,
        backgroundColor: color
    }}
/>
);

export default StockDesriptionHeader;

