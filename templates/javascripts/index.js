$(document).ready(function () {
  var timeData = [],
    temperatureData = [],
    humidityData = [];
  var data = {
    labels: timeData,
    datasets: [
      {
        fill: false,
        label: '{{.LeftLabel}}',
        yAxisID: '{{.LeftLabel}}',
        borderColor: "rgba(255, 204, 0, 1)",
        pointBoarderColor: "rgba(255, 204, 0, 1)",
        backgroundColor: "rgba(255, 204, 0, 0.4)",
        pointHoverBackgroundColor: "rgba(255, 204, 0, 1)",
        pointHoverBorderColor: "rgba(255, 204, 0, 1)",
        data: temperatureData
      },
      {
        fill: false,
        label: '{{.RightLabel}}',
        yAxisID: '{{.RightLabel}}',
        borderColor: "rgba(24, 120, 240, 1)",
        pointBoarderColor: "rgba(24, 120, 240, 1)",
        backgroundColor: "rgba(24, 120, 240, 0.4)",
        pointHoverBackgroundColor: "rgba(24, 120, 240, 1)",
        pointHoverBorderColor: "rgba(24, 120, 240, 1)",
        data: humidityData
      }
    ]
  }

  var basicOption = {
    title: {
      display: true,
      text: '{{.GraphLabel}}',
      fontSize: 36
    },
    scales: {
      yAxes: [{
        id: '{{.LeftLabel}}',
        type: 'linear',
        scaleLabel: {
          labelString: '{{.LeftAxisLabel}}',
          display: true
        },
        position: 'left',
      }, {
          id: '{{.RightLabel}}',
          type: 'linear',
          scaleLabel: {
            labelString: '{{.RightAxisLabel}}',
            display: true
          },
          position: 'right'
        }]
    }
  }

  //Get the context of the canvas element we want to select
  var ctx = document.getElementById("myChart").getContext("2d");
  var optionsNoAnimation = { animation: false }
  Chart.defaults.global.defaultFontFamily = 'Helvetica, Arial, sans-serif';
  var myLineChart = new Chart(ctx, {
    type: 'line',
    data: data,
    options: basicOption
  });

  {{/* if scheme is meant to be secure, then wss will be used, otherwise ws */}}
  var ws = new WebSocket("{{.WebsocketsScheme}}"+ location.host + "/data");
    
  ws.onopen = function () {
    console.log('Successfully connect WebSocket');
  }
  ws.onmessage = function (message) {
    console.log('receive message' + message.data);
    try {
      var obj = JSON.parse(message.data);
      if(!obj.time || !obj.{{.LeftJSKey}}) {
        return;
      }
      timeData.push(obj.time);
      temperatureData.push(obj.{{.LeftJSKey}});
      // only keep no more than 50 points in the line chart
      const maxLen = 50;
      var len = timeData.length;
      if (len > maxLen) {
        timeData.shift();
        temperatureData.shift();
      }

      if (obj.{{.RightJSKey}}) {
        humidityData.push(obj.{{.RightJSKey}});
      }
      if (humidityData.length > maxLen) {
        humidityData.shift();
      }

      myLineChart.update();
    } catch (err) {
      console.error(err);
    }
  }
});
