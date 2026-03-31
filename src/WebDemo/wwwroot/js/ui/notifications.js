// Lightweight alert/notification helpers

export function showAlert(elementId, message, type) {
    const wrapper = document.getElementById(elementId);
    if (!wrapper) return;

    const textEl = document.getElementById(elementId + '-text');
    if (textEl) textEl.textContent = message;

    // Set alert type (alert-danger, alert-success, etc.)
    wrapper.className = wrapper.className.replace(/alert-(success|danger|warning|info)/g, '');
    wrapper.classList.add('alert-' + (type || 'info'));
    wrapper.classList.remove('d-none');
}

export function hideAlert(elementId) {
    const wrapper = document.getElementById(elementId);
    if (wrapper) wrapper.classList.add('d-none');
}

export function hideAllAlerts() {
    hideAlert('error-alert');
    hideAlert('conversion-alert');
    hideAlert('data-load-alert');
}
