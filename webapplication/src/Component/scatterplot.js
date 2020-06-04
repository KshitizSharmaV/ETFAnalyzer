import React from 'react';
import {ScatterChart, Scatter, XAxis, YAxis, CartesianGrid, Tooltip, Legend} from 'recharts';

class ScatterPlot extends React.Component {
  
  constructor(props){
    super(props);
  }

  state ={
    data:[{x: 100, y: 200}, {x: 120, y: 100},
          {x: 170, y: 300}, {x: 140, y: 250},
          {x: 150, y: 400}, {x: 110, y: 280}]
    }

  render() {
    return (
        <ScatterChart width={500} height={300} margin={{top: 20, right: 20, bottom: 20, left: 20}}>
          <CartesianGrid />
          <XAxis dataKey={'Net Asset Value Change%'} type="number" name='Net Asset Value Chage %' unit='%'/>
          <YAxis dataKey={'ETF Change Price %'} type="number" name='ETF Change Price %' unit='%'/>
          <Scatter name='A school' data={this.props.data} fill='#292b2c'/>
          <Tooltip cursor={{strokeDasharray: '3 3'}}/>
        </ScatterChart>
      );
      
  }
}

export default ScatterPlot;