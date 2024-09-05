document.addEventListener('DOMContentLoaded', function() {
    console.log("Menu script: DOM fully loaded and parsed.");

    const links = window.AppData.links;

    if (links.length > 0) {
        links.forEach(link => {
            link.addEventListener('click', function() {
                links.forEach(link => link.classList.remove('selected'));
                this.classList.add('selected');

                const docId = this.getAttribute('data-document-id');
                showLoading();

                fetch(`/indicators/query/${docId}`)
                    .then(response => response.json())
                    .then(data => {
                        updateDocumentDetails(data);
                        hideLoading();
                    })
                    .catch(hideLoading);

                fetch(`/indicators/api/prompts/${docId}`)
                    .then(response => response.json())
                    .then(data => {
                        populatePrompts(data);
                        updateTaskEvents();
                        hideLoading();
                    })
                    .catch(hideLoading);
            });
        });
    }
});

function updateDocumentDetails(data) {
    const docTitle = window.AppData.mainContainer.querySelector('#document-title');
    const docPath = window.AppData.mainContainer.querySelector('#document-path');
    const releaseDateSelect = window.AppData.releaseDateSelect;
    const runAIButton = window.AppData.runAIButton;

    if (docTitle && docPath && releaseDateSelect && runAIButton) {
        docTitle.textContent = data.document_name.toUpperCase() + ' - ' + data.source_name.toUpperCase();
        docTitle.setAttribute('data-document-id', data.document_id);
        docPath.href = data.path;
        docPath.textContent = data.path;

        releaseDateSelect.innerHTML = data.release_dates.map(date => {
            const dateStr = String(date).replace(/(\d{4})(\d{2})(\d{2})/, '$1/$2/$3');
            return `<option value="${dateStr}">${dateStr}</option>`;
        }).join('');
        releaseDateSelect.setAttribute('data-document-id', data.document_id);

        runAIButton.setAttribute('data-document-id', data.document_id);
    }
}

function populatePrompts(data) {
    const macroEnvDesc = document.getElementById('macro-env-desc');
    const audience = document.getElementById('audience');
    const objective = document.getElementById('objective');
    const constraintsLangUsage = document.getElementById('constraints-lang-usage');
    const constraintsLangStyle = document.getElementById('constraints-lang-style');
    const tasksContainer = window.AppData.tasksContainer;

    if (macroEnvDesc && audience && objective && constraintsLangUsage && constraintsLangStyle && tasksContainer) {
        macroEnvDesc.value = data.format_output_macro_environment_impacts_description || '';
        audience.value = data.audience || '';
        objective.value = data.objective || '';
        constraintsLangUsage.value = data.constraints_language_usage || '';
        constraintsLangStyle.value = data.constraints_language_style || '';

        tasksContainer.innerHTML = '';
        data.tasks.forEach(task => {
            if (task) {
                const taskDiv = document.createElement('div');
                taskDiv.className = 'task';
                taskDiv.innerHTML = `<textarea class="task-input">${task}</textarea><button class="delete-task" type="button">-</button>`;
                tasksContainer.appendChild(taskDiv);
            }
        });
    }
}

function updateTaskEvents() {
    document.querySelectorAll('.delete-task').forEach(button => {
        button.addEventListener('click', function() {
            const tasksContainer = window.AppData.tasksContainer;
            if (tasksContainer) {
                const taskCount = tasksContainer.querySelectorAll('.task').length;
                if (taskCount > 1) {
                    this.parentElement.remove();
                } else {
                    alert('At least one task is required.');
                }
            }
        });
    });
}
