/* CPI Line Chart */
new Chart(document.getElementById("pricesChart"), {
    type: "line",
    data: {
        labels: pricesData.map(x => x.period),
        datasets: [{
            label: "CPI",
            data: pricesData.map(x => x.total),
            borderWidth: 2,
            tension: 0.3
        }]
    }
});

/* GDP Pie Chart */
new Chart(document.getElementById("gdpPieChart"), {
    type: "doughnut",
    data: {
        labels: gdpData.map(x => x.province),
        datasets: [{
            data: gdpData.map(x => x.total)
        }]
    }
});

/* Region Radar */
new Chart(document.getElementById("regionChart"), {
    type: "radar",
    data: {
        labels: gdpData.map(x => x.province),
        datasets: [{
            label: "GDP Region",
            data: gdpData.map(x => x.total)
        }]
    }
});
