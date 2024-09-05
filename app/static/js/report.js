document.addEventListener('DOMContentLoaded', function() {
    console.log('Report script: DOM fully loaded and parsed.');

    const releaseDateSelect = window.AppData.releaseDateSelect;

    if (releaseDateSelect) {
        releaseDateSelect.addEventListener('change', function() {
            const docId = this.getAttribute('data-document-id');
            const date = this.value.replace(/\//g, '');
            showLoading();

            fetch(`/indicators/query/${docId}?date=${date}`)
                .then(response => response.json())
                .then(data => {
                    updateDocumentSummaries(data);
                    hideLoading();
                }).catch(hideLoading);
        });
    }
});

function updateDocumentSummaries(data) {
    const enSummary = document.getElementById('en-summary');
    const ptSummary = document.getElementById('pt-summary');
    const keyTakeawaysContainer = document.getElementById('key-takeaways');

    if (enSummary && ptSummary && keyTakeawaysContainer) {
        enSummary.textContent = data.en_summary;
        ptSummary.textContent = data.pt_summary;

        keyTakeawaysContainer.innerHTML = data.key_takeaways.map(kt =>
            `<h4 class="key-takeaway-title text-lg">${kt.title}</h4><p>${kt.content}</p>`).join('');
    }
}
