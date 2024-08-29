document.addEventListener('DOMContentLoaded', function () {
    const releaseDateSelect = document.getElementById('release-date-select');

    if (releaseDateSelect) {
        releaseDateSelect.addEventListener('change', function () {
            const docId = this.getAttribute('data-document-id');
            const date = this.value.replace(/\//g, '');
            showLoading();
            fetch(`/indicators/query/${docId}?date=${date}`)
                .then(response => response.json())
                .then(data => {
                    document.getElementById('en-summary').textContent = data.en_summary;
                    document.getElementById('pt-summary').textContent = data.pt_summary;
                    document.getElementById('key-takeaways').innerHTML = data.key_takeaways.map(kt => `<h4 class="key-takeaway-title text-lg">${kt.title}</h4><p>${kt.content}</p>`).join('');
                    hideLoading();
                }).catch(hideLoading);
        });
    }

    function showLoading() {
        document.getElementById('overlay').style.display = 'block';
        document.getElementById('loading-indicator').style.display = 'block';
    }

    function hideLoading() {
        document.getElementById('overlay').style.display = 'none';
        document.getElementById('loading-indicator').style.display = 'none';
    }
});
