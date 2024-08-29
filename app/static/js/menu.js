document.addEventListener('DOMContentLoaded', function () {
    const links = document.querySelectorAll('.indicator-link');

    if (links.length > 0) {
        links.forEach(link => {
            link.addEventListener('click', function () {
                links.forEach(link => link.classList.remove('selected'));
                this.classList.add('selected');

                const docId = this.getAttribute('data-document-id');
                console.log(`Document ID clicked: ${docId}`);
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

    function showLoading() {
        document.getElementById('overlay').style.display = 'block';
        document.getElementById('loading-indicator').style.display = 'block';
    }

    function hideLoading() {
        document.getElementById('overlay').style.display = 'none';
        document.getElementById('loading-indicator').style.display = 'none';
    }

    function updateDocumentDetails(data) {
        document.getElementById('document-title').textContent = data.document_name.toUpperCase() + ' - ' + data.source_name.toUpperCase();
        document.getElementById('document-title').setAttribute('data-document-id', data.document_id);
        document.getElementById('document-path').href = data.path;
        document.getElementById('document-path').textContent = data.path;
        const releaseDateSelect = document.getElementById('release-date-select');
        releaseDateSelect.innerHTML = data.release_dates.map(date => {
            const dateStr = String(date).replace(/(\d{4})(\d{2})(\d{2})/, '$1/$2/$3');
            return `<option value="${dateStr}">${dateStr}</option>`;
        }).join('');
        releaseDateSelect.setAttribute('data-document-id', data.document_id);
        document.getElementById('run-ai-agent').setAttribute('data-document-id', data.document_id);
        document.getElementById('en-summary').textContent = data.en_summary;
        document.getElementById('pt-summary').textContent = data.pt_summary;
        document.getElementById('key-takeaways').innerHTML = data.key_takeaways.map(kt => `<h4 class="key-takeaway-title text-lg">${kt.title}</h4><p>${kt.content}</p>`).join('');
        document.getElementById('main-container').style.display = 'block';
        document.getElementById('crud-container').style.display = 'block';
        document.getElementById('placeholder').style.display = 'none';
    }

    function populatePrompts(data) {
        document.getElementById('macro-env-desc').value = data.format_output_macro_environment_impacts_description || '';
        document.getElementById('audience').value = data.audience || '';
        document.getElementById('objective').value = data.objective || '';
        document.getElementById('constraints-lang-usage').value = data.constraints_language_usage || '';
        document.getElementById('constraints-lang-style').value = data.constraints_language_style || '';

        const tasksContainer = document.getElementById('tasks-container');
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

    function updateTaskEvents() {
        document.querySelectorAll('.delete-task').forEach(button => {
            button.addEventListener('click', function () {
                const tasksContainer = document.getElementById('tasks-container');
                const taskCount = tasksContainer.querySelectorAll('.task').length;
                if (taskCount > 1) {
                    this.parentElement.remove();
                } else {
                    alert('At least one task is required.');
                }
            });
        });
    }
});
