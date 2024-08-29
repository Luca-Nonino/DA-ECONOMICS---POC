document.addEventListener('DOMContentLoaded', function () {
    const themeSwitcher = document.getElementById('theme-switcher');

    if (themeSwitcher) {
        themeSwitcher.addEventListener('click', function () {
            document.body.classList.toggle('dark-mode');
            document.body.classList.toggle('light-mode');
            document.querySelectorAll('.button').forEach(button => {
                if (document.body.classList.contains('dark-mode')) {
                    button.style.backgroundColor = '#228b22';
                    button.style.color = 'white';
                } else {
                    button.style.backgroundColor = '#d2691e';
                    button.style.color = 'white';
                }
            });
        });
    }
});
