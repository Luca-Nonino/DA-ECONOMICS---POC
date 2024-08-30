document.addEventListener('DOMContentLoaded', function () {
    console.log("DOM fully loaded and parsed. Initializing AppData...");

    // Initialize AppData with checks and logging
    const AppData = {
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
        closeSummaryModal: document.getElementsByClassName("close-summary-modal")[0],
        coinsMonitorLink: document.getElementById('coins-monitor-link'),
        coinsContainer: document.getElementById('coins-monitor-container'),
        coinsDateSelect: document.getElementById('coins-date-select'),
        runCoinsAIButton: document.getElementById('run-coins-ai-agent'),
        copyCoinsReportButton: document.getElementById('copy-coins-report'),
        coinsReportContent: document.getElementById('coins-report-content'),
    };

    // Log for debugging
    console.log('AppData initialized with elements:', AppData);

    // Helper function to hide all containers
    function hideAllContainers() {
        AppData.mainContainer.style.display = 'none';
        AppData.crudContainer.style.display = 'none';
        AppData.coinsContainer.style.display = 'none';
        AppData.placeholder.style.display = 'block'; // Show placeholder if no container is active
    }

    // Theme switcher logic
    if (AppData.themeSwitcher) {
        AppData.themeSwitcher.addEventListener('click', function () {
            document.body.classList.toggle('dark-mode');
            document.body.classList.toggle('light-mode');
            document.querySelectorAll('.button').forEach(button => {
                const isDarkMode = document.body.classList.contains('dark-mode');
                button.style.backgroundColor = isDarkMode ? '#228b22' : '#d2691e';
                button.style.color = 'white';
            });
        });
    } else {
        console.warn("Theme switcher element not found.");
    }

    // Menu logic for indicators
    if (AppData.links.length > 0) {
        AppData.links.forEach(link => {
            link.addEventListener('click', function () {
                AppData.links.forEach(link => link.classList.remove('selected'));
                this.classList.add('selected');
                const docId = this.getAttribute('data-document-id');
                console.log(`Fetching data for Document ID: ${docId}`);
                showLoading();

                // Hide all containers first
                hideAllContainers();

                // Show relevant containers for indicators
                AppData.mainContainer.style.display = 'block';
                AppData.crudContainer.style.display = 'block';
                AppData.placeholder.style.display = 'none';

                fetch(`/indicators/query/${docId}`)
                    .then(response => response.json())
                    .then(data => {
                        document.getElementById('document-title').textContent = `${data.document_name.toUpperCase()} - ${data.source_name.toUpperCase()}`;
                        document.getElementById('document-title').setAttribute('data-document-id', data.document_id);
                        document.getElementById('document-path').href = data.path;
                        document.getElementById('document-path').textContent = data.path;
                        AppData.releaseDateSelect.innerHTML = data.release_dates.map(date => {
                            const dateStr = String(date).replace(/(\d{4})(\d{2})(\d{2})/, '$1/$2/$3');
                            return `<option value="${dateStr}">${dateStr}</option>`;
                        }).join('');
                        AppData.releaseDateSelect.setAttribute('data-document-id', data.document_id);
                        AppData.runAIButton.setAttribute('data-document-id', data.document_id);
                        document.getElementById('en-summary').textContent = data.en_summary;
                        document.getElementById('pt-summary').textContent = data.pt_summary;
                        document.getElementById('key-takeaways').innerHTML = data.key_takeaways.map(kt => `<h4 class="key-takeaway-title text-lg">${kt.title}</h4><p>${kt.content}</p>`).join('');
                        hideLoading();
                    }).catch(error => {
                        console.error("Error fetching document data:", error);
                        hideLoading();
                    });

                fetch(`/indicators/api/prompts/${docId}`)
                    .then(response => response.json())
                    .then(data => {
                        document.getElementById('macro-env-desc').value = data.format_output_macro_environment_impacts_description || '';
                        document.getElementById('audience').value = data.audience || '';
                        document.getElementById('objective').value = data.objective || '';
                        document.getElementById('constraints-lang-usage').value = data.constraints_language_usage || '';
                        document.getElementById('constraints-lang-style').value = data.constraints_language_style || '';
                        AppData.tasksContainer.innerHTML = '';
                        data.tasks.forEach(task => {
                            const taskDiv = document.createElement('div');
                            taskDiv.className = 'task';
                            taskDiv.innerHTML = `<textarea class="task-input">${task}</textarea><button class="delete-task" type="button">-</button>`;
                            AppData.tasksContainer.appendChild(taskDiv);
                        });
                        updateTaskEvents();
                    }).catch(error => {
                        console.error("Error fetching prompt data:", error);
                        hideLoading();
                    });
            });
        });
    } else {
        console.warn('No indicator links found.');
    }

    // Menu logic for FX Monitor
    if (AppData.coinsMonitorLink) {
        AppData.coinsMonitorLink.addEventListener('click', function () {
            // Hide all containers first
            hideAllContainers();

            // Show relevant container for FX Monitor
            AppData.coinsContainer.style.display = 'block';
            fetchAvailableDates();
        });
    } else {
        console.warn("Coins Monitor link not found.");
    }

    // Release Date change logic for indicators
    if (AppData.releaseDateSelect) {
        AppData.releaseDateSelect.addEventListener('change', function () {
            const docId = this.getAttribute('data-document-id');
            const date = this.value.replace(/\//g, '');
            showLoading();

            fetch(`/indicators/query/${docId}?date=${date}`)
                .then(response => response.json())
                .then(data => {
                    document.getElementById('en-summary').textContent = data.en_summary;
                    document.getElementById('pt-summary').textContent = data.pt_summary;
                    document.getElementById('key-takeaways').innerHTML = data.key_takeaways.map(kt =>
                        `<h4 class="key-takeaway-title text-lg">${kt.title}</h4><p>${kt.content}</p>`).join('');
                    hideLoading();
                }).catch(error => {
                    console.error('Error fetching data for release date:', error);
                    hideLoading();
                });
        });
    } else {
        console.warn('Release date select element not found.');
    }

    // AI Prompt logic for indicators
    if (AppData.runAIButton) {
        AppData.runAIButton.addEventListener('click', function () {
            const docId = this.getAttribute('data-document-id');
            showLoading();
            fetch(`/indicators/update/?id=${docId}`)
                .then(response => response.text())
                .then(html => {
                    AppData.modalBody.innerHTML = html;
                    AppData.modal.style.display = "block";
                    AppData.span.onclick = function () {
                        AppData.modal.style.display = "none";
                        fetchDocumentData(docId);
                    };
                    window.onclick = function (event) {
                        if (event.target == AppData.modal) {
                            AppData.modal.style.display = "none";
                            fetchDocumentData(docId);
                        }
                    };
                }).catch(error => {
                    console.error("Error running AI Agent:", error);
                    hideLoading();
                });
        });
    } else {
        console.warn("AI Run button not found.");
    }

    // Add Task logic for indicators
    if (AppData.addTaskButton) {
        AppData.addTaskButton.addEventListener('click', function () {
            const taskCount = AppData.tasksContainer.querySelectorAll('.task').length;
            if (taskCount < 5) {
                const newTask = document.createElement('div');
                newTask.className = 'task';
                newTask.innerHTML = `<textarea class="task-input"></textarea><button class="delete-task" type="button">-</button>`;
                AppData.tasksContainer.appendChild(newTask);
                updateTaskEvents();
            } else {
                alert('Maximum 5 tasks allowed.');
            }
        });
    } else {
        console.warn("Add Task button not found.");
    }

    // Save Changes logic for indicators
    if (AppData.saveChangesButton) {
        AppData.saveChangesButton.addEventListener('click', function () {
            const docId = document.getElementById('document-title').getAttribute('data-document-id');
            const macroEnvDesc = document.getElementById('macro-env-desc').value;
            const audience = document.getElementById('audience').value;
            const objective = document.getElementById('objective').value;
            const constraintsLangUsage = document.getElementById('constraints-lang-usage').value;
            const constraintsLangStyle = document.getElementById('constraints-lang-style').value;
            const tasks = Array.from(document.querySelectorAll('.task-input')).map(input => input.value);

            showLoading();
            fetch(`/indicators/api/update_all`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    prompt_id: docId,
                    macro_env_desc: macroEnvDesc,
                    audience,
                    objective,
                    constraints_lang_usage: constraintsLangUsage,
                    constraints_lang_style: constraintsLangStyle,
                    tasks
                })
            })
                .then(response => response.json())
                .then(data => {
                    if (data.status === 'success') {
                        alert('Changes saved successfully!');
                    } else {
                        alert('Failed to save changes.');
                    }
                    hideLoading();
                }).catch(error => {
                    console.error("Error saving changes:", error);
                    hideLoading();
                });
        });
    } else {
        console.warn("Save Changes button not found.");
    }

    // FX Monitor Date Select and Report logic
    if (AppData.coinsDateSelect) {
        AppData.coinsDateSelect.addEventListener('change', function () {
            const selectedDate = AppData.coinsDateSelect.value;
            fetchReportForDate(selectedDate);
        });
    } else {
        console.warn("Coins date select element not found.");
    }

    if (AppData.runCoinsAIButton) {
        AppData.runCoinsAIButton.addEventListener('click', function () {
            const selectedDate = AppData.coinsDateSelect.value;
            runAIForDate(selectedDate);
        });
    } else {
        console.warn("Run Coins AI button not found.");
    }

    if (AppData.copyCoinsReportButton) {
        AppData.copyCoinsReportButton.addEventListener('click', function () {
            const reportText = AppData.coinsReportContent.innerText;
            copyToClipboard(reportText);
            alert("Report copied to clipboard!");
        });
    } else {
        console.warn("Copy Coins Report button not found.");
    }

    // Helper functions
    function fetchAvailableDates() {
        fetch('/coin_monitor/available_dates')
            .then(response => response.json())
            .then(data => {
                AppData.coinsDateSelect.innerHTML = data.available_dates.map(date =>
                    `<option value="${date}">${date}</option>`).join('');
                if (data.available_dates.length > 0) {
                    fetchReportForDate(data.available_dates[0]);
                }
            })
            .catch(error => console.error('Error fetching available dates:', error));
    }

    function fetchReportForDate(date) {
        fetch(`/coin_monitor/report/${date}`)
            .then(response => response.text())
            .then(report => {
                AppData.coinsReportContent.innerHTML = formatReportText(report);
            })
            .catch(error => console.error('Error fetching report:', error));
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
            .catch(error => console.error('Error running AI agent:', error));
    }

    function updateTaskEvents() {
        document.querySelectorAll('.delete-task').forEach(button => {
            button.addEventListener('click', function () {
                const taskCount = AppData.tasksContainer.querySelectorAll('.task').length;
                if (taskCount > 1) {
                    this.parentElement.remove();
                } else {
                    alert('At least one task is required.');
                }
            });
        });
    }

    function showLoading() {
        AppData.overlay.style.display = 'block';
        AppData.loadingIndicator.style.display = 'block';
    }

    function hideLoading() {
        AppData.overlay.style.display = 'none';
        AppData.loadingIndicator.style.display = 'none';
    }

    function formatReportText(text) {
        // Remove first and last line if they contain ```
        text = text.replace(/^\`\`\`[\r\n]+|[\r\n]+\`\`\`$/g, '');
        
        // Replace markdown asterisks and underscores with HTML equivalents
        text = text
            .replace(/\*(.*?)\*/g, '<strong>$1</strong>') // Bold for *text*
            .replace(/_(.*?)_/g, '<em>$1</em>') // Italic for _text_
            .replace(/---/g, '<hr>') // Horizontal line for ---
            .replace(/\n/g, '<br>'); // Line breaks

        return text;
    }

    function copyToClipboard(text) {
        const tempInput = document.createElement('textarea');
        tempInput.style.position = 'fixed';
        tempInput.style.opacity = '0';
        tempInput.value = text;
        document.body.appendChild(tempInput);
        tempInput.select();
        document.execCommand('Copy');
        document.body.removeChild(tempInput);
    }

    function fetchDocumentData(docId) {
        fetch(`/indicators/query/${docId}`)
            .then(response => response.json())
            .then(data => {
                document.getElementById('en-summary').textContent = data.en_summary;
                document.getElementById('pt-summary').textContent = data.pt_summary;
                document.getElementById('key-takeaways').innerHTML = data.key_takeaways.map(kt => `<h4 class="key-takeaway-title text-lg">${kt.title}</h4><p>${kt.content}</p>`).join('');
                AppData.testPromptButton.click();
            }).catch(error => {
                console.error('Error fetching document data:', error);
                hideLoading();
            });
    }

    window.onclick = function (event) {
        if (event.target == AppData.modal) {
            AppData.modal.style.display = "none";
            fetchDocumentData(document.getElementById('document-title').getAttribute('data-document-id'));
        }
        if (AppData.generatedSummaryModal && event.target == AppData.generatedSummaryModal) {
            AppData.generatedSummaryModal.style.display = "none";
            document.getElementById('header').style.zIndex = 999;
        }
    };

    if (AppData.closeSummaryModal) {
        AppData.closeSummaryModal.addEventListener('click', function () {
            AppData.generatedSummaryModal.style.display = "none";
            document.getElementById('header').style.zIndex = 999;
        });
    } else {
        console.warn("Close Summary Modal button not found.");
    }
});
