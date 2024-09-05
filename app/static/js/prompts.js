// prompts.js
document.addEventListener('DOMContentLoaded', function() {
    console.log('Prompts script: DOM fully loaded and parsed.');

    const testPromptButton = window.AppData.testPromptButton;
    const saveChangesButton = window.AppData.saveChangesButton;
    const addTaskButton = window.AppData.addTaskButton;
    const generatedSummaryModal = window.AppData.generatedSummaryModal;
    const closeSummaryModal = window.AppData.closeSummaryModal;
    const generatedSummaryContent = window.AppData.generatedSummaryContent;
    const copyGeneratedSummaryButton = window.AppData.copyGeneratedSummaryButton;


    if (testPromptButton) {
        testPromptButton.addEventListener('click', function () {
            const docId = document.getElementById('document-title').getAttribute('data-document-id');
            const releaseDate = document.getElementById('release-date-select').value.replace(/\//g, '');
            const enSummary = document.getElementById('en-summary').textContent;
            const ptSummary = document.getElementById('pt-summary').textContent;
            const keyTakeaways = Array.from(document.querySelectorAll('#key-takeaways .key-takeaway-title')).map(title => {
                return {
                    title: title.textContent,
                    content: title.nextElementSibling.textContent
                };
            });

            const promptData = {
                macro_env_desc: document.getElementById('macro-env-desc').value,
                audience: document.getElementById('audience').value,
                objective: document.getElementById('objective').value,
                constraints_lang_usage: document.getElementById('constraints-lang-usage').value,
                constraints_lang_style: document.getElementById('constraints-lang-style').value,
                tasks: Array.from(document.querySelectorAll('.task-input')).map(task => task.value)
            };

            showLoading();

            fetch('/indicators/api/generate_pt_summary', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    doc_id: docId,
                    release_date: releaseDate,
                    en_summary: enSummary,
                    pt_summary: ptSummary,
                    key_takeaways: keyTakeaways,
                    prompt_data: promptData
                })
            })
            .then(response => response.json())
            .then(data => {
                generatedSummaryContent.innerHTML = formatMarkdown(data.pt_summary);
                generatedSummaryModal.style.display = "block";

                closeSummaryModal.onclick = function() {
                    generatedSummaryModal.style.display = "none";
                };

                window.onclick = function(event) {
                    if (event.target == generatedSummaryModal) {
                        generatedSummaryModal.style.display = "none";
                    }
                };

                copyGeneratedSummaryButton.addEventListener('click', function () {
                    const summaryText = `Summary - Portuguese:\n${data.pt_summary}`;
                    copyToClipboard(summaryText);
                    alert("Summary copied to clipboard!");
                });

                hideLoading();
            })
            .catch(error => {
                console.error('Error generating PT summary:', error);
                alert('Failed to generate PT summary.');
                hideLoading();
            });
        });
    }

    if (saveChangesButton) {
        saveChangesButton.addEventListener('click', function () {
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
            }).catch(hideLoading);
        });
    }

    if (addTaskButton) {
        addTaskButton.addEventListener('click', function () {
            const tasksContainer = window.AppData.tasksContainer;
            const taskCount = tasksContainer.querySelectorAll('.task').length;
            if (taskCount < 5) {
                const newTask = document.createElement('div');
                newTask.className = 'task';
                newTask.innerHTML = `<textarea class="task-input"></textarea><button class="delete-task" type="button">-</button>`;
                tasksContainer.appendChild(newTask);
                updateTaskEvents();
            } else {
                alert('Maximum 5 tasks allowed.');
            }
        });
    }

    function updateTaskEvents() {
        document.querySelectorAll('.delete-task').forEach(button => {
            button.addEventListener('click', function () {
                const tasksContainer = window.AppData.tasksContainer;
                const taskCount = tasksContainer.querySelectorAll('.task').length;
                if (taskCount > 1) {
                    this.parentElement.remove();
                } else {
                    alert('At least one task is required.');
                }
            });
        });
    }

    function showLoading() {
        window.AppData.overlay.style.display = 'block';
        window.AppData.loadingIndicator.style.display = 'block';
    }

    function hideLoading() {
        window.AppData.overlay.style.display = 'none';
        window.AppData.loadingIndicator.style.display = 'none';
    }

    function formatMarkdown(text) {
        return text
            .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
            .replace(/__(.*?)__/g, '<em>$1</em>');
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

    updateTaskEvents();
});