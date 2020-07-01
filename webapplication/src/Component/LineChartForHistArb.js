import React, { useState } from "react";
import {LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer} from 'recharts';


const LineChartForHistArbJs = (props) => {
  const { data } = props;
  
  return (
    <ResponsiveContainer width="100%" height={250} padding={{ top: 0, right: 0, left: 0, bottom: 0 }}>
      <LineChart data={data}>
        <XAxis className="recharts-cartesian-axis-tick" dataKey="Time" />
        <YAxis className="recharts-cartesian-axis-tick" />
        <Tooltip />
        <Legend />
        <Line type="monotone" dataKey="$Arbitrage" stroke="#8884d8" dot={false} />
      </LineChart>
    </ResponsiveContainer>
  );
};

export default LineChartForHistArbJs;
