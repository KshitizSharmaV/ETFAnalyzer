import React, { PureComponent } from 'react';
import { PieChart, Pie, Sector, Cell, ResponsiveContainer } from 'recharts';

const renderActiveShape = (props) => {
  const RADIAN = Math.PI / 180;
  const {
    cx, cy, midAngle, innerRadius, outerRadius, startAngle, endAngle,
    fill, payload, value,
  } = props;
  const sin = Math.sin(-RADIAN * midAngle);
  const cos = Math.cos(-RADIAN * midAngle);
  const sx = cx + (outerRadius + 10) * cos;
  const sy = cy + (outerRadius + 10) * sin;
  const mx = cx + (outerRadius + 30) * cos;
  const my = cy + (outerRadius + 30) * sin;
  const ex = mx + (cos >= 0 ? 1 : -1) * 11;
  const ey = my;
  const textAnchor = cos >= 0 ? 'start' : 'end';

  return (
    <g>
      <text x={cx} y={cy} dy={8} textAnchor="middle" fill={fill}>{payload.name}</text>
      <Sector
        cx={cx}
        cy={cy}
        innerRadius={innerRadius}
        outerRadius={outerRadius}
        startAngle={startAngle}
        endAngle={endAngle}
        fill={fill}
      />
      <Sector
        cx={cx}
        cy={cy}
        startAngle={startAngle}
        endAngle={endAngle}
        innerRadius={outerRadius + 6}
        outerRadius={outerRadius + 10}
        fill={fill}
      />
      <path d={`M${sx},${sy}L${mx},${my}L${ex},${ey}`} stroke={fill} fill="none" />
      <circle cx={ex} cy={ey} r={2} fill={fill} stroke="none" />
      <text x={ex + (cos >= 0 ? 1 : -1) * 12} y={ey} textAnchor={textAnchor} fill="#FFFFFF">{`${value}`}</text>
    </g>
  );
};


class PieChartGraph extends PureComponent {
  constructor(props) {
      super(props);
      this.state = {
          data: [],
          chartname:'',
          activeIndex: 0,
          COLORS     : ['#0088FE', '#00C49F', '#FFBB28', '#FF8042']
      };
  }

  async componentDidUpdate(prevProps) {
    if((this.props.data !== prevProps.data)){
      await this.setState({data : []});
      for (let key in this.props.data){
        await this.setState({
            data : [...this.state.data, {'name': key, 'value': this.props.data[key][this.props.element]}],
            chartname:this.props.chartname
        })
      }
    }
  }

  async componentDidMount (){
    for (let key in this.props.data){
      await this.setState({
          data : [...this.state.data, {'name': key, 'value': this.props.data[key][this.props.element]}]
      })
    }
  }
  
  onPieEnter = (data, index) => {
    this.setState({
      activeIndex: index,
    });
  };
  
  render() {
    return (

    <ResponsiveContainer width="100%" height={250} padding={{ top: 1, right: 30, left: 20, bottom: 1 }}>
      <PieChart>
        <Pie
        activeIndex={this.state.activeIndex}
        activeShape={renderActiveShape}
        data={this.state.data}
        innerRadius={30}
        outerRadius={80}
        fill="#8884d8"
        dataKey="value"
        onMouseEnter={this.onPieEnter}
        >
          {
              this.state.data.map((entry, index) => <Cell key={index} fill={this.state.COLORS[index % this.state.COLORS.length]}/>)
          }
        </Pie>
      </PieChart>
    </ResponsiveContainer>
    );
  }
}

export default PieChartGraph;