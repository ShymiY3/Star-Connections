// JavaScript function to handle form submission
document.getElementById('form').addEventListener('submit', function(event) {
    // Disable the submit button to prevent multiple submissions
    document.getElementById('button').disabled = true;

    // Display the "calculating" overlay
    document.getElementById('searching-overlay').style.display = 'block';

    document.getElementById('form').submit();

    // Prevent the default form submission
    event.preventDefault();
});
