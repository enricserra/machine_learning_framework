var plotNumeric = function(canvasId, min, max, label, data, beginAtZero) {
    var ctx = document.getElementById(canvasId);
    var colorsAndBorders = generateColors(data.length);
    var myChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: generateLabels(min, max, data.length),
            datasets: [{
                label: label,
                data: data,
                backgroundColor: colorsAndBorders[0],
                borderColor: colorsAndBorders[1],
                borderWidth: 1
            }, {
                label: label,
                data: data,
                type: "line",
                borderWidth: 1
            }]
        },
        options: {
            scales: {
                yAxes: [{
                    ticks: {
                        beginAtZero: beginAtZero
                    }
                }]
            }
        }
    });
}

var generateLabels = function(min, max, length) {
    var increment = ((max -min)/length);
    var realNumbers = [min]
    for(var i =0; i < length; i++) {
        realNumbers.push(realNumbers[realNumbers.length - 1] + increment);
    }
    return realNumbers.map(Math.round)
}

var generateColors = function(length) {
    var returnColorList = [];
    var returnBorderColorList = []
    for(var i = 0; i < length; i++) {
        returnColorList.push('rgba(255, 255, 0, 1)')
        returnBorderColorList.push('rgba(0,0,0,1)')
    }
    return [returnColorList, returnBorderColorList];
}

var getUrlFromFilePath = function(filepath) {
    return "http://10.0.32.112:8200/" + filepath;
}
