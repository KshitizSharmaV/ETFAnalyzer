import React from "react";
import {
  ScatterChart,
  Scatter,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
} from "recharts";

class ScatterPlot extends React.Component {
  constructor(props) {
    super(props);
  }

  render() {
    return (
      <ScatterChart width={400} height={220}>
        <CartesianGrid />
        <XAxis
          stroke="#58FF33"
          dataKey={"Net Asset Value Change%"}
          type="number"
          name="Net Asset Value Chage %"
          unit="%"
        />
        <YAxis
          stroke="#58FF33"
          dataKey={"ETF Change Price %"}
          type="number"
          name="ETF Change Price %"
          unit="%"
        />
        <Scatter name="A school" data={this.props.data} fill="#3336FF" />
        <Tooltip cursor={{ strokeDasharray: "3 3" }} />
      </ScatterChart>
    );
  }
}

export default ScatterPlot;
