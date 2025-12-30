// JavaScript for WBS Management System
document.addEventListener('DOMContentLoaded', function() {
    // Performance tracking
    const startTime = performance.now();
    
    // Loading state management
    const loadingOverlay = document.getElementById('loadingOverlay');
    const loadingSpinner = document.getElementById('loadingSpinner');
    
    // Show loading on page navigation
    function showPageLoading() {
        if (loadingOverlay && loadingSpinner) {
            loadingOverlay.style.display = 'block';
            loadingSpinner.style.display = 'block';
        }
    }
    
    // Hide loading
    function hidePageLoading() {
        if (loadingOverlay && loadingSpinner) {
            loadingOverlay.style.display = 'none';
            loadingSpinner.style.display = 'none';
        }
    }
    
    // Add loading states to all navigation links
    const navLinks = document.querySelectorAll('a.page-link, .pagination a, form');
    navLinks.forEach(link => {
        if (link.tagName === 'FORM') {
            link.addEventListener('submit', function() {
                showPageLoading();
            });
        } else {
            link.addEventListener('click', function() {
                showPageLoading();
            });
        }
    });
    
    // Hide loading when page is loaded
    window.addEventListener('load', function() {
        hidePageLoading();
        const loadTime = performance.now() - startTime;
        console.log(`Page loaded in ${loadTime.toFixed(2)}ms`);
    });
    // Search functionality
    const searchInput = document.getElementById('searchInput');
    const searchSuggestions = document.getElementById('searchSuggestions');
    let searchTimeout;

    if (searchInput && searchSuggestions) {
        // Search input event listener
        searchInput.addEventListener('input', function() {
            clearTimeout(searchTimeout);
            const query = this.value.trim();

            if (query.length < 2) {
                hideSuggestions();
                return;
            }

            // Debounce search requests
            searchTimeout = setTimeout(() => {
                fetchSuggestions(query);
            }, 300);
        });

        // Hide suggestions when clicking outside
        document.addEventListener('click', function(e) {
            if (!searchInput.contains(e.target) && !searchSuggestions.contains(e.target)) {
                hideSuggestions();
            }
        });

        // Handle keyboard navigation
        searchInput.addEventListener('keydown', function(e) {
            const suggestions = searchSuggestions.querySelectorAll('.suggestion-item');
            let currentIndex = Array.from(suggestions).findIndex(item => 
                item.classList.contains('selected')
            );

            switch(e.key) {
                case 'ArrowDown':
                    e.preventDefault();
                    if (currentIndex < suggestions.length - 1) {
                        selectSuggestion(suggestions, currentIndex + 1);
                    }
                    break;
                
                case 'ArrowUp':
                    e.preventDefault();
                    if (currentIndex > 0) {
                        selectSuggestion(suggestions, currentIndex - 1);
                    }
                    break;
                
                case 'Enter':
                    e.preventDefault();
                    if (currentIndex >= 0 && suggestions[currentIndex]) {
                        suggestions[currentIndex].click();
                    } else {
                        // Submit the form
                        searchInput.closest('form').submit();
                    }
                    break;
                
                case 'Escape':
                    hideSuggestions();
                    break;
            }
        });
    }

    // Fetch search suggestions via API
    function fetchSuggestions(query) {
        // Use existing loading indicators instead of separate spinner
        const searchBtn = document.querySelector('button[type="submit"]');
        if (searchBtn) {
            searchBtn.disabled = true;
            searchBtn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Searching...';
        }
        
        fetch(`/api/search?q=${encodeURIComponent(query)}`)
            .then(response => response.json())
            .then(data => {
                displaySuggestions(data);
            })
            .catch(error => {
                console.error('Search error:', error);
                hideSuggestions();
            })
            .finally(() => {
                // Reset search button
                if (searchBtn) {
                    searchBtn.disabled = false;
                    searchBtn.innerHTML = '<i class="bi bi-search me-1"></i>Search';
                }
            });
    }

    // Display search suggestions
    function displaySuggestions(suggestions) {
        if (!suggestions || suggestions.length === 0) {
            hideSuggestions();
            return;
        }

        const html = suggestions.map(item => `
            <div class="suggestion-item" data-code="${item.code}">
                <div class="suggestion-code">${highlightMatch(item.code, searchInput.value)}</div>
                <div class="suggestion-desc">${highlightMatch(item.description, searchInput.value)}</div>
            </div>
        `).join('');

        searchSuggestions.innerHTML = html;
        searchSuggestions.style.display = 'block';

        // Add click event listeners to suggestions
        searchSuggestions.querySelectorAll('.suggestion-item').forEach(item => {
            item.addEventListener('click', function() {
                const code = this.dataset.code;
                searchInput.value = code;
                hideSuggestions();
                // Navigate to the item detail page
                window.location.href = `/item/${encodeURIComponent(code)}`;
            });

            item.addEventListener('mouseenter', function() {
                removeSelection();
                this.classList.add('selected');
            });
        });
    }

    // Show loading spinner in suggestions
    function showLoadingSpinner() {
        searchSuggestions.innerHTML = `
            <div class="suggestion-item text-center">
                <span class="loading-spinner me-2"></span>
                Searching...
            </div>
        `;
        searchSuggestions.style.display = 'block';
    }

    // Hide search suggestions
    function hideSuggestions() {
        searchSuggestions.style.display = 'none';
        searchSuggestions.innerHTML = '';
    }

    // Highlight matching text in suggestions
    function highlightMatch(text, query) {
        if (!text || !query) return text;
        
        const regex = new RegExp(`(${escapeRegExp(query)})`, 'gi');
        return text.replace(regex, '<mark>$1</mark>');
    }

    // Escape special characters for regex
    function escapeRegExp(string) {
        return string.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    }

    // Select suggestion by index
    function selectSuggestion(suggestions, index) {
        removeSelection();
        if (suggestions[index]) {
            suggestions[index].classList.add('selected');
            // Scroll into view if needed
            suggestions[index].scrollIntoView({ block: 'nearest' });
        }
    }

    // Remove selection from all suggestions
    function removeSelection() {
        searchSuggestions.querySelectorAll('.suggestion-item').forEach(item => {
            item.classList.remove('selected');
        });
    }

    // Smooth scrolling for anchor links
    document.querySelectorAll('a[href^="#"]').forEach(anchor => {
        anchor.addEventListener('click', function (e) {
            e.preventDefault();
            const target = document.querySelector(this.getAttribute('href'));
            if (target) {
                target.scrollIntoView({
                    behavior: 'smooth',
                    block: 'start'
                });
            }
        });
    });

    // Auto-hide alerts after 5 seconds
    document.querySelectorAll('.alert').forEach(alert => {
        if (!alert.querySelector('.btn-close')) {
            setTimeout(() => {
                alert.classList.add('fade');
                setTimeout(() => alert.remove(), 150);
            }, 5000);
        }
    });

    // Add loading states to buttons
    document.querySelectorAll('button[type="submit"], .btn-loading').forEach(btn => {
        btn.addEventListener('click', function() {
            if (!this.classList.contains('disabled')) {
                const originalText = this.innerHTML;
                this.innerHTML = '<span class="loading-spinner me-1"></span>Loading...';
                this.classList.add('disabled');
                
                // Reset after 10 seconds as fallback
                setTimeout(() => {
                    this.innerHTML = originalText;
                    this.classList.remove('disabled');
                }, 10000);
            }
        });
    });

    // Copy to clipboard functionality
    window.copyToClipboard = function(text) {
        if (navigator.clipboard && window.isSecureContext) {
            navigator.clipboard.writeText(text).then(function() {
                showToast('Copied to clipboard!', 'success');
            }).catch(function(err) {
                console.error('Failed to copy: ', err);
                fallbackCopyTextToClipboard(text);
            });
        } else {
            fallbackCopyTextToClipboard(text);
        }
    };

    // Fallback copy method for older browsers
    function fallbackCopyTextToClipboard(text) {
        const textArea = document.createElement("textarea");
        textArea.value = text;
        textArea.style.top = "0";
        textArea.style.left = "0";
        textArea.style.position = "fixed";
        document.body.appendChild(textArea);
        textArea.focus();
        textArea.select();
        
        try {
            document.execCommand('copy');
            showToast('Copied to clipboard!', 'success');
        } catch (err) {
            console.error('Fallback: Oops, unable to copy', err);
            showToast('Failed to copy to clipboard', 'error');
        }
        
        document.body.removeChild(textArea);
    }

    // Show toast notification
    function showToast(message, type = 'info') {
        const toast = document.createElement('div');
        toast.className = `alert alert-${type === 'error' ? 'danger' : type} position-fixed`;
        toast.style.cssText = 'top: 20px; right: 20px; z-index: 1100; min-width: 300px;';
        toast.innerHTML = `
            <div class="d-flex align-items-center">
                <i class="bi bi-${type === 'success' ? 'check-circle' : type === 'error' ? 'exclamation-triangle' : 'info-circle'}-fill me-2"></i>
                <span>${message}</span>
                <button type="button" class="btn-close ms-auto" onclick="this.parentElement.parentElement.remove()"></button>
            </div>
        `;
        
        document.body.appendChild(toast);
        
        // Auto remove after 3 seconds
        setTimeout(() => {
            if (toast.parentNode) {
                toast.remove();
            }
        }, 3000);
    }

    // Initialize tooltips if Bootstrap is loaded
    if (typeof bootstrap !== 'undefined' && bootstrap.Tooltip) {
        const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
        tooltipTriggerList.map(function (tooltipTriggerEl) {
            return new bootstrap.Tooltip(tooltipTriggerEl);
        });
    }

    // Performance monitoring (optional)
    if ('performance' in window) {
        window.addEventListener('load', function() {
            setTimeout(function() {
                const perfData = performance.getEntriesByType('navigation')[0];
                if (perfData) {
                    console.log(`Page loaded in ${Math.round(perfData.loadEventEnd - perfData.fetchStart)}ms`);
                }
            }, 0);
        });
    }

    console.log('WBS Management System initialized successfully');
});

// Global utility functions
window.WBSUtils = {
    formatDate: function(dateString) {
        if (!dateString) return 'N/A';
        try {
            const date = new Date(dateString);
            return date.toLocaleDateString('en-US', {
                year: 'numeric',
                month: 'short',
                day: 'numeric'
            });
        } catch (e) {
            return dateString;
        }
    },
    
    truncateText: function(text, length = 100) {
        if (!text || text.length <= length) return text;
        return text.substring(0, length) + '...';
    }
};