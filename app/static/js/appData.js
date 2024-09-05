document.addEventListener('DOMContentLoaded', function() {
    console.log("DOM fully loaded and parsed.");

    // Initialize AppData after DOM is ready
    function initializeAppData() {
        window.AppData = {
            themeSwitcher: document.getElementById('theme-switcher'),
            links: document.querySelectorAll('.indicator-link'),
            releaseDateSelect: document.getElementById('release-date-select'),
            runAIButton: document.getElementById('run-ai-agent'),
            tasksContainer: document.getElementById('tasks-container'),
            placeholder: document.getElementById('placeholder'),
            mainContainer: document.getElementById('main-container'),
            crudContainer: document.getElementById('crud-container'),
            modal: document.getElementById("myModal"),
            modalBody: document.getElementById('modal-body'),
            generatedSummaryModal: document.getElementById('generated-summary-modal'),
            generatedSummaryContent: document.getElementById('generated-summary-content'),
            copyGeneratedSummaryButton: document.getElementById('copy-generated-summary'),
            copySummaryButton: document.getElementById('copy-summary'),
            saveChangesButton: document.getElementById('save-changes'),
            addTaskButton: document.getElementById('add-task'),
            loadingIndicator: document.getElementById('loading-indicator'),
            overlay: document.getElementById('overlay'),
            span: document.getElementsByClassName("close")[0],
            closeSummaryModal: document.getElementsByClassName("close-summary-modal")[0]
        };

        console.log('AppData initialized with elements:', window.AppData);
    }

    initializeAppData();
});
