import React from "react";
import { Table } from "react-bootstrap";

const EtfArbitrageTable = (props) => {
  if (props.data == {} || props.data == undefined) {
    return "Loading";
  }

  const { data } = props;

  const getKeys = function (someJSON) {
    return Object.keys(someJSON);
  };

  const getRowsData = () => {
    var Time = getKeys(props.data.Time);

    return Time.map((key, index) => {
      //console.log(key);
      let cls = "";
      if (props.data["Over Bought/Sold"][key] == "Over Bought") {
        cls = "Green";
      } else if (props.data["Over Bought/Sold"][key] == "Over Sold") {
        cls = "Red";
      } else {
        cls = "";
      }
      return (
        <tr key={index}>
          <td className={cls}>{props.data["Time"][key]}</td>
          <td className={cls}>{props.data["$Arbitrage"][key]}</td>
          <td className={cls}>{props.data["$Spread"][key]}</td>
          <td className={cls}>{props.data["Absolute Arbitrage"][key]}</td>
          <td className={cls}>{props.data["Over Bought/Sold"][key]}</td>
          <td>{props.data["Etf Mover"][key]}</td>
          <td>{props.data["Most Change%"][key]}</td>
          <td>{props.data["T"][key]}</td>
          <td>{props.data["T+1"][key]}</td>
        </tr>
      );
    });
  };

  return (
    <Table
      size="sm"
      striped
      bordered
      hover
      variant="dark"
      className="font-size-sm"
    >
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
      <tbody>{getRowsData()}</tbody>
    </Table>
  );
};

export default EtfArbitrageTable;
