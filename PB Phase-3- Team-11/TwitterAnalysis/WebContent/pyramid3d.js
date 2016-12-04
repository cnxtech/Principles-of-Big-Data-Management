$(function () {

    $('#container').highcharts({
        chart: {
            type: 'pyramid',
            marginRight: 100
        },
        title: {
            text: 'Top 10 Users Tweeted on Diseases',
            x: -50
        },
        plotOptions: {
            series: {
                dataLabels: {
                    enabled: true,
                    format: '<b>{point.name}</b> ({point.y:,.0f})',
                    color: (Highcharts.theme && Highcharts.theme.contrastTextColor) || 'black',
                    softConnector: true
                }
            }
        },
        legend: {
            enabled: false
        },
        series: [{
            name: 'Tweet Count',
            data: [
                ['HIV/AIDS News',74], 
                ['Jen Schoenberger',323],
                ['Juan J. Ramirez',50],
                ['Madhu Pai',90],
                ['Ken Pledger',25],
                ['Corine K',470],
                ['EWU Disease Ecology',76],
                ['Jay Peepefanio',50], 
                ['No More Asthma',99],
                ['James Wolter',590], 
            ]
        }]
    });
});