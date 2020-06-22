import React from 'react';
import Table from 'react-bootstrap/Table'
import '../static/css/TableStyle.css'

var stringConstructor = "test".constructor;
var arrayConstructor = [].constructor;
var objectConstructor = ({}).constructor;

function whatIsIt(object) {
  if (object === null) {
    return "null";
  }
  if (object === undefined) {
    return "undefined";
  }
  if (object.constructor === stringConstructor) {
    return "String";
  }
  if (object.constructor === arrayConstructor) {
    return "Array";
  }
  if (object.constructor === objectConstructor) {
    return "Object";
  }
  return "don't know";
}

const AppTable = (props) => {
  // getting all te keys to the json data to diectly fetch the data later
  const getKeys = function(someJSON){
    return Object.keys(someJSON);
  }

  const MainKeys = getKeys(props.data);
  
  // getting the headings for the heading of the table
  const getHeader = function(){
    // console.log(whatIsIt(props.data[MainKeys[0]]));
    var keys = (whatIsIt(props.data[MainKeys[0]]) === "Object") ? getKeys(props.data[MainKeys[0]]) : [];
    keys.unshift("");
    // console.log(keys);
    return keys.map((key, index)=>{
      // console.log(key);
      return <th key={key}>{key}</th>
    })
  }
  
  // getting data for each of the rows
  const getRowsData = function(){
    // var keys = (whatIsIt(props.data[MainKeys[0]]) != "Object") ? getKeys(props.data[MainKeys[0]]) : [];
    return MainKeys.map((Key1, index) => {
      var row = (typeof(props.data[Key1]) == Object) ? props.data[Key1].values() : props.data[Key1];
      // console.log(row);
    
      if (props.clickableTable=='True'){
        //console.log("Clickable was called");
        return <RenderRowClickable k={Key1} key={index} data={row} submitFn={props.submitFn}/>
      } else{
        //console.log("None-Clickable was called");
        return <RenderRow k={Key1} key={index} data={row} />
      }
    }) 
  }
  
  const TableStyling = {
    fontSize: '13px'
  };

  return (
    <div className="Table">
      <Table size="sm" striped bordered hover variant="dark" style={TableStyling}>
      <thead className="TableHead">
        <tr>{getHeader()}</tr>
      </thead>
      <tbody>
        {getRowsData()}
      </tbody>
      </Table>
    </div>          
  );
}

// functional Component to render one row at a time
const RenderRow = (props) =>{
  if (whatIsIt(props.data) !== "Object"){
    return( 
      <tr>
        <td className="Main">{props.k}</td>
        <td>{props.data}</td>
      </tr>
    )
  }
  else {
    return(
      <tr>
        <td className="Main">{props.k}</td>
        {
          Object.keys(props.data).map((key, i) => (
            <td key={i}>{props.data[key]}</td>
          ))
        }
      </tr>
    );
  }
}


// functional Component to render one row at a time
const RenderRowClickable = (props) =>{
  if (whatIsIt(props.data) !== "Object"){
    return( 
      <tr>
        <td className="Main">{props.k}</td>
        <td>{props.data}</td>
      </tr>
    )
  }
  else {
    return(
      <tr onClick={() => props.submitFn(props.k)}>
        <td className="Main">{props.k}</td>
        {
          Object.keys(props.data).map((key, i) => (
            <td key={i}>{props.data[key]}</td>
          ))
        }
      </tr>
    );
  }
}

export default AppTable