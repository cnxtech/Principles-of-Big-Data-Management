google.charts.load('current', {'packages':['geochart']});
google.charts.setOnLoadCallback(drawRegionsMap);

function drawRegionsMap() {

    var data = google.visualization.arrayToDataTable([
        ['Country', 'Count'],
        ['US', 920],
        ['GB', 152],
        ['FR', 41],
        ['CA', 33],
        ['ES', 24],
        ['AU', 20],
        ['ID', 13],
        ['MX', 13],
        ['CM', 13],
        ['AR', 12],
        ['ZA', 10],
        ['NG', 8],
        ['CO', 8],
        ['IN', 200],
        ['MY', 6],
        ['BR', 6],
        ['PH', 6],
        ['AT', 6],
        ['VE', 4],
        ['NL', 4],
    ]);

    var options = {};

    var chart = new google.visualization.GeoChart(document.getElementById('regions_div'));

    chart.draw(data, options);
}
