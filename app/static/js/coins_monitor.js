document.addEventListener('DOMContentLoaded', function() {
    console.log('Coins Monitor script: DOM fully loaded and parsed.');

    const coinsMonitorLink = document.getElementById('coins-monitor-link');
    const coinsContainer = document.getElementById('coins-monitor-container');
    const coinsDateSelect = document.getElementById('coins-date-select');
    const runCoinsAIButton = document.getElementById('run-coins-ai-agent');
    const copyCoinsReportButton = document.getElementById('copy-coins-report');
    const coinsReportContent = document.getElementById('coins-report-content');

    if (coinsMonitorLink) {
        coinsMonitorLink.addEventListener('click', function() {
            document.querySelectorAll('.box').forEach(box => box.classList.add('hidden'));
            coinsContainer.classList.remove('hidden');
            fetchAvailableDates();
        });
    }

    if (coinsDateSelect) {
        coinsDateSelect.addEventListener('change', function() {
            const selectedDate = coinsDateSelect.value;
            fetchReportForDate(selectedDate);
        });
    }

    if (runCoinsAIButton) {
        runCoinsAIButton.addEventListener('click', function() {
            const selectedDate = coinsDateSelect.value;
            runAIForDate(selectedDate);
        });
    }

    if (copyCoinsReportButton) {
        copyCoinsReportButton.addEventListener('click', function() {
            const reportText = coinsReportContent.textContent;
            copyToClipboard(reportText);
            alert("Report copied to clipboard!");
        });
    }
});

function fetchAvailableDates() {
    fetch('/coin_monitor/available_dates')
        .then(response => response.json())
        .then(data => {
            coinsDateSelect.innerHTML = data.available_dates.map(date =>
                `<option value="${date}">${date}</option>`).join('');
            if (data.available_dates.length > 0) {
                fetchReportForDate(data.available_dates[0]);
            }
        })
        .catch(console.error);
}

function fetchReportForDate(date) {
    fetch(`/coin_monitor/report/${date}`)
        .then(response => response.text())
        .then(report => coinsReportContent.innerHTML = formatReportText(report))
        .catch(console.error);
}

function runAIForDate(date) {
    fetch('/coin_monitor/update_report', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({ date: date })
    })
    .then(response => response.json())
    .then(data => {
        alert(data.message);
        fetchReportForDate(date);
    })
    .catch(console.error);
}
