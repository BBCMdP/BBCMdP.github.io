// Runtime configuration for the Proteomes DB frontend.
//
// For local dev:
//   window.BBC_PROTEOMES_API_BASE = 'http://localhost:8000';
//
// For GitHub Pages (must be HTTPS to avoid mixed-content errors):
//   window.BBC_PROTEOMES_API_BASE = 'https://your-api.example.org';

// Current deployed API (temporary hostname while DuckDNS CAA is flaky):
window.BBC_PROTEOMES_API_BASE = 'https://bbcproteomesdb.mdp.edu.ar';

// Default behavior:
// - If running on localhost: use http://localhost:8000
// - Otherwise: leave unset and let proteomes.js show an actionable error
