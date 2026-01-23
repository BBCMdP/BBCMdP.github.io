// BBC Proteomes Database - JavaScript
// API endpoint resolution
// - Prefer runtime config: window.BBC_PROTEOMES_API_BASE
// - Default to localhost in local dev
function resolveApiBase() {
    const configured = (typeof window !== 'undefined' && window.BBC_PROTEOMES_API_BASE)
        ? String(window.BBC_PROTEOMES_API_BASE).trim()
        : '';
    if (configured) return configured.replace(/\/$/, '');

    const isLocalhost = (location.hostname === 'localhost' || location.hostname === '127.0.0.1');
    if (isLocalhost) return 'http://localhost:8000';

    // In production (e.g., GitHub Pages), you must provide BBC_PROTEOMES_API_BASE via assets/config.js.
    return '';
}

const API_BASE = resolveApiBase();

// Column configuration: grouping, display labels, defaults, mandatory, hidden
const COLUMN_CONFIG = {
    'Proteome': [
        { key: 'hash', label: 'Hash', mandatory: true, default: true },
        { key: 'Origin CP', label: 'Proteome source', default: false },
        { key: 'AssemblyID', label: 'AssemblyID', default: false },
        { key: 'web', label: 'URL', default: false },
        { key: "In NCBI's ref seq", label: 'NCBI refseq', default: false },
        { key: 'Species', label: 'Species', default: false },
        { key: 'taxID', label: 'taxID', default: true },
        { key: 'species_taxid', label: 'taxID at species level', default: false },
        { key: 'current_scientific_name', label: "NCBI's Curr. Sci. Name", default: true },
        { key: 'common_names', label: 'Common name', default: false },
        { key: 'group_name', label: "NCBI's Group name", default: false },
        { key: 'informal_clade', label: 'Informal clade', default: false },
        { key: 'code_vFV', label: "Proteome's code renamed version FV", default: false },
    ],
    'Stats': [
        { key: 'num_seqs', label: 'Original Proteome Seq #', default: false },
        { key: 'sum_len', label: 'sum_len', hidden: true },
        { key: 'min_len', label: 'min_len', hidden: true },
        { key: 'avg_len', label: 'Sequence length (mean)', default: false },
        { key: 'max_len', label: 'max_len', hidden: true },
        { key: 'num_seqs_snip_processed', label: 'SNIP accepted Seq #', default: false },
        { key: 'sum_len_snip_processed', label: 'sum_len_snip_processed', hidden: true },
        { key: 'min_len_snip_processed', label: 'min_len_snip_processed', hidden: true },
        { key: 'avg_len_snip_processed', label: 'SNIP sequence length (mean)', default: false },
        { key: 'max_len_snip_processed', label: 'max_len_snip_processed', hidden: true },
        { key: 'post_snip', label: 'SNIP accepted seqs %', default: false },
        { key: 'Complete BUSCO Domain', label: 'BUSCO Domain: Complete', default: true },
        { key: 'Single BUSCO Domain', label: 'BUSCO Domain: Single', default: false },
        { key: 'Duplicated BUSCO Domain', label: 'BUSCO Domain: Duplicated', default: false },
        { key: 'Fragmented BUSCO Domain', label: 'BUSCO Domain: Fragmented', default: false },
        { key: 'Missing BUSCO Domain', label: 'BUSCO Domain: Missing', default: false },
        { key: 'Complete BUSCO Kingdom', label: 'BUSCO Kingdom: Complete', default: false },
        { key: 'Single BUSCO Kingdom', label: 'BUSCO Kingdom: Single', default: false },
        { key: 'Duplicated BUSCO Kingdom', label: 'BUSCO Kingdom: Duplicated', default: false },
        { key: 'Fragmented BUSCO Kingdom', label: 'BUSCO Kingdom: Fragmented', default: false },
        { key: 'Missing BUSCO Kingdom', label: 'BUSCO Kingdom: Missing', default: false },
    ],
    'Location in Server': [
        { key: 'File_name', label: 'Original file name', default: false },
        { key: 'Filepath_original', label: 'Original filepath', default: false },
        { key: 'File_snip_name', label: 'SNIP file name', default: false },
        { key: 'Filepath_snip_processed', label: 'SNIP filepath', default: false },
        { key: 'Filepath_renamed_vFV', label: 'Final renamed filepath', default: false },
    ],
    'Taxonomy': [
        { key: 'Domain', label: 'Domain', default: false },
        { key: 'Realm', label: 'Realm', default: false },
        { key: 'Kingdom', label: 'Kingdom', default: true },
        { key: 'Subkingdom', label: 'Subkingdom', default: false },
        { key: 'Superphylum', label: 'Superphylum', default: false },
        { key: 'Phylum', label: 'Phylum', default: true },
        { key: 'Subphylum', label: 'Subphylum', default: false },
        { key: 'Infraphylum', label: 'Infraphylum', default: false },
        { key: 'Superclass', label: 'Superclass', default: false },
        { key: 'Class', label: 'Class', default: true },
        { key: 'Subclass', label: 'Subclass', default: false },
        { key: 'Infraclass', label: 'Infraclass', default: false },
        { key: 'Cohort', label: 'Cohort', default: false },
        { key: 'Subcohort', label: 'Subcohort', default: false },
        { key: 'Superorder', label: 'Superorder', default: false },
        { key: 'Order', label: 'Order', default: true },
        { key: 'Suborder', label: 'Suborder', default: false },
        { key: 'Infraorder', label: 'Infraorder', default: false },
        { key: 'Parvorder', label: 'Parvorder', default: false },
        { key: 'Superfamily', label: 'Superfamily', default: false },
        { key: 'Family', label: 'Family', default: true },
        { key: 'Subfamily', label: 'Subfamily', default: false },
        { key: 'Tribe', label: 'Tribe', default: false },
        { key: 'Subtribe', label: 'Subtribe', default: false },
        { key: 'Genus', label: 'Genus', default: false },
        { key: 'Subgenus', label: 'Subgenus', default: false },
        { key: 'Section', label: 'Section', default: false },
        { key: 'Subsection', label: 'Subsection', default: false },
        { key: 'Series', label: 'Series', default: false },
        { key: 'Subseries', label: 'Subseries', default: false },
        { key: 'Species_group', label: 'Species_group', default: false },
        { key: 'Species_subgroup', label: 'Species_subgroup', default: false },
        { key: 'Forma_specialis', label: 'Forma_specialis', default: false },
        { key: 'Subspecies', label: 'Subspecies', default: false },
        { key: 'VarietasSubvariety', label: 'VarietasSubvariety', default: false },
        { key: 'Forma', label: 'Forma', default: false },
        { key: 'Serogroup', label: 'Serogroup', default: false },
        { key: 'Serotype', label: 'Serotype', default: false },
        { key: 'Strain', label: 'Strain', default: false },
        { key: 'Isolate', label: 'Isolate', default: false },
    ],
};

// Helper function to get display label for a column key
function getColumnLabel(key) {
    for (const section of Object.keys(COLUMN_CONFIG)) {
        const item = COLUMN_CONFIG[section].find(col => col.key === key);
        if (item) return item.label || key;
    }
    return key;
}

// State
let currentPage = 0;
let limit = 50;
let totalResults = 0;
let availableColumns = [];
let selectedColumns = new Set();
let sortColumn = null;  // Current sort column
let sortOrder = 'asc';  // Current sort order (asc/desc)

// DOM Elements
const searchInput = document.getElementById('searchInput');
const collectionSelect = document.getElementById('collectionSelect');
const taxonomyLevelSelect = document.getElementById('taxonomyLevelSelect');
const taxonomyNameSelect = document.getElementById('taxonomyNameSelect');
const buscoMetricSelect = document.getElementById('buscoMetricSelect');
const buscoMinValue = document.getElementById('buscoMinValue');
const applyFiltersBtn = document.getElementById('applyFiltersBtn');
const resetFiltersBtn = document.getElementById('resetFiltersBtn');
const columnCheckboxes = document.getElementById('columnCheckboxes');
const selectAllColumnsBtn = document.getElementById('selectAllColumnsBtn');
const deselectAllColumnsBtn = document.getElementById('deselectAllColumnsBtn');
const applyFiltersBtn2 = document.getElementById('applyFiltersBtn2');
const resetFiltersBtn2 = document.getElementById('resetFiltersBtn2');
const resultsContainer = document.getElementById('resultsContainer');
const loadingIndicator = document.getElementById('loadingIndicator');
const resultCount = document.getElementById('resultCount');
const paginationContainer = document.getElementById('paginationContainer');
const pageInfo = document.getElementById('pageInfo');
const prevPageBtn = document.getElementById('prevPageBtn');
const nextPageBtn = document.getElementById('nextPageBtn');
const exportTsvBtn = document.getElementById('exportTsvBtn');
const lifemapBtn = document.getElementById('lifemapBtn');

// Initialize
async function init() {
    try {
        if (!API_BASE) {
            throw new Error('Missing API base URL. Set window.BBC_PROTEOMES_API_BASE in assets/config.js');
        }
        await loadColumns();
        await loadCollections();
        await loadTaxonomyLevels();
        setupEventListeners();
        renderColumnCheckboxes();
    } catch (error) {
        const hint = API_BASE
            ? ('Make sure the API server is running at ' + API_BASE)
            : 'Set window.BBC_PROTEOMES_API_BASE in assets/config.js (and ensure it is HTTPS on GitHub Pages).';
        showError('Failed to initialize. ' + hint);
        console.error(error);
    }
}

// API Calls
async function apiGet(endpoint, params = {}) {
    const url = new URL(API_BASE + endpoint);
    Object.keys(params).forEach(key => {
        if (params[key] !== null && params[key] !== undefined && params[key] !== '') {
            url.searchParams.append(key, params[key]);
        }
    });
    const response = await fetch(url);
    if (!response.ok) throw new Error(`API error: ${response.statusText}`);
    return response.json();
}

async function loadColumns() {
        const data = await apiGet('/columns');
        availableColumns = data.columns || [];
        // Initialize selectedColumns per config defaults and availability
        selectedColumns.clear();
        for (const section of Object.keys(COLUMN_CONFIG)) {
            for (const item of COLUMN_CONFIG[section]) {
                if (item.hidden) continue;
                if (!availableColumns.includes(item.key)) continue; // only include existing columns
                if (item.default || item.mandatory) {
                    selectedColumns.add(item.key);
                }
            }
        }
        // Ensure mandatory columns always present even if not in SHOW COLUMNS (defensive)
        for (const item of COLUMN_CONFIG['Proteome']) {
            if (item.mandatory) selectedColumns.add(item.key);
        }
}

async function loadCollections() {
    const collections = await apiGet('/collections');
    collections.forEach(col => {
        const option = document.createElement('option');
        option.value = col.name;
        option.textContent = `${col.name} (${col.count})`;
        collectionSelect.appendChild(option);
    });
}

async function loadTaxonomyLevels() {
    const levels = await apiGet('/taxonomy/levels');
    levels.forEach(level => {
        const option = document.createElement('option');
        option.value = level.level;
        option.textContent = `${level.level} (${level.count})`;
        taxonomyLevelSelect.appendChild(option);
    });
}

async function loadTaxonomyNames(level) {
    taxonomyNameSelect.innerHTML = '<option value="">Select taxon...</option>';
    if (!level) {
        taxonomyNameSelect.disabled = true;
        return;
    }
    const names = await apiGet(`/taxonomy/${level}`);
    names.forEach(name => {
        const option = document.createElement('option');
        option.value = name;
        option.textContent = name;
        taxonomyNameSelect.appendChild(option);
    });
    taxonomyNameSelect.disabled = false;
}

async function loadProteomes() {
    showLoading(true);
    try {
        const params = {
            q: searchInput.value.trim(),
            collection: collectionSelect.value,
            taxonomy_level: taxonomyLevelSelect.value,
            taxonomy_name: taxonomyNameSelect.value,
            busco_column: buscoMetricSelect.value,
            busco_min_value: buscoMinValue.value,
            columns: Array.from(selectedColumns).join(','),
            sort_column: sortColumn,
            sort_order: sortOrder,
            limit: limit,
            offset: currentPage * limit
        };
        
        const data = await apiGet('/proteomes', params);
        totalResults = data.total;
        renderResults(data.items);
        updatePagination();
    } catch (error) {
        showError('Failed to load proteomes: ' + error.message);
    } finally {
        showLoading(false);
    }
}

function exportData(format) {
    const params = new URLSearchParams({
        q: searchInput.value.trim(),
        collection: collectionSelect.value,
        taxonomy_level: taxonomyLevelSelect.value,
        taxonomy_name: taxonomyNameSelect.value,
        busco_column: buscoMetricSelect.value,
        busco_min_value: buscoMinValue.value,
        sort_column: sortColumn,
        sort_order: sortOrder,
        columns: Array.from(selectedColumns).join(','),
        format: format
    });
    
    // Remove empty params
    for (let [key, value] of [...params.entries()]) {
        if (!value) params.delete(key);
    }
    
    const url = `${API_BASE}/export?${params.toString()}`;
    window.open(url, '_blank');
}

async function openLifemap() {
    showLoading(true);
    try {
        // Fetch all filtered results with species_taxid column
        const params = {
            q: searchInput.value.trim(),
            collection: collectionSelect.value,
            taxonomy_level: taxonomyLevelSelect.value,
            taxonomy_name: taxonomyNameSelect.value,
            busco_column: buscoMetricSelect.value,
            busco_min_value: buscoMinValue.value,
            columns: 'species_taxid',  // Only need species_taxid
            limit: 500,  // Get up to 500 results
            offset: 0
        };
        
        const data = await apiGet('/proteomes', params);
        
        if (!data.items || data.items.length === 0) {
            alert('No results to display in Lifemap');
            return;
        }
        
        // Extract species_taxid values and remove nulls/duplicates
        const taxids = [...new Set(
            data.items
                .map(item => item.species_taxid)
                .filter(id => id !== null && id !== undefined && id !== '')
        )];
        
        if (taxids.length === 0) {
            alert('No valid species_taxid values found in the filtered results');
            return;
        }
        
        // Build Lifemap URL
        const lifemapUrl = `https://lifemap.cnrs.fr/tree?efficiency-mode=true&tool=subtree&subtree=${taxids.join(',')}`;
        window.open(lifemapUrl, '_blank');
        
    } catch (error) {
        showError('Failed to generate Lifemap URL: ' + error.message);
    } finally {
        showLoading(false);
    }
}

// UI Rendering
function renderColumnCheckboxes() {
    columnCheckboxes.innerHTML = '';
    const grid = document.createElement('div');
    grid.className = 'grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6';

    const makeSection = (title, items, isTaxonomy = false) => {
        const sectionDiv = document.createElement('div');
        const header = document.createElement('h4');
        header.className = 'font-semibold mb-2';
        header.textContent = title;
        sectionDiv.appendChild(header);

        const list = document.createElement('div');
        // If Taxonomy, use 2-column layout; otherwise single column
        list.className = isTaxonomy ? 'grid grid-cols-2 gap-3' : 'flex flex-col gap-2';

        items.forEach(item => {
            if (item.hidden) return;
            if (!availableColumns.includes(item.key)) return;

            const row = document.createElement('div');
            row.className = 'flex items-center gap-2';

            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.id = `col_${item.key}`;
            checkbox.value = item.key;
            checkbox.checked = selectedColumns.has(item.key);
            checkbox.className = 'rounded text-primary focus:ring-primary';
            if (item.mandatory) {
                checkbox.checked = true;
                checkbox.disabled = true; // cannot unselect mandatory
            }
            checkbox.addEventListener('change', () => {
                if (checkbox.checked) {
                    selectedColumns.add(item.key);
                } else {
                    if (item.mandatory) {
                        checkbox.checked = true; // enforce
                    } else {
                        selectedColumns.delete(item.key);
                    }
                }
            });

            const label = document.createElement('label');
            label.htmlFor = `col_${item.key}`;
            label.textContent = item.label || item.key;
            label.className = 'text-sm cursor-pointer';

            row.appendChild(checkbox);
            row.appendChild(label);
            list.appendChild(row);
        });

        sectionDiv.appendChild(list);
        return sectionDiv;
    };

    for (const sectionName of Object.keys(COLUMN_CONFIG)) {
        const isTax = sectionName === 'Taxonomy';
        grid.appendChild(makeSection(sectionName, COLUMN_CONFIG[sectionName], isTax));
    }

    columnCheckboxes.appendChild(grid);
}

function renderResults(items) {
    if (items.length === 0) {
        resultsContainer.innerHTML = '<p class="text-gray-500 text-center py-12">No results found</p>';
        resultCount.textContent = '';
        return;
    }
    
    const cols = Array.from(selectedColumns);
    let html = '<table class="min-w-full text-sm"><thead class="bg-gray-100 dark:bg-gray-800"><tr>';
    cols.forEach(col => {
        const label = getColumnLabel(col);
        const isSorted = sortColumn === col;
        const sortIndicator = isSorted ? (sortOrder === 'asc' ? ' ↑' : ' ↓') : '';
        const cursor = 'cursor-pointer hover:bg-gray-200 dark:hover:bg-gray-700';
        html += `<th class="px-4 py-2 text-left font-semibold ${cursor}" data-col="${col}">${label}${sortIndicator}</th>`;
    });
    html += '</tr></thead><tbody>';
    
    items.forEach((item, idx) => {
        const rowClass = idx % 2 === 0 ? 'bg-white dark:bg-gray-900' : 'bg-gray-50 dark:bg-gray-800';
        html += `<tr class="${rowClass}">`;
        cols.forEach(col => {
            const value = item[col] !== null && item[col] !== undefined ? item[col] : '';
            html += `<td class="px-4 py-2 border-t border-gray-200 dark:border-gray-700">${value}</td>`;
        });
        html += '</tr>';
    });
    
    html += '</tbody></table>';
    resultsContainer.innerHTML = html;
    
    // Attach click handlers to table headers for sorting
    const headers = resultsContainer.querySelectorAll('th[data-col]');
    headers.forEach(header => {
        header.addEventListener('click', () => {
            const col = header.getAttribute('data-col');
            if (sortColumn === col) {
                // Toggle sort order
                sortOrder = sortOrder === 'asc' ? 'desc' : 'asc';
            } else {
                // New column, default to asc
                sortColumn = col;
                sortOrder = 'asc';
            }
            currentPage = 0;
            loadProteomes();
        });
    });
    
    const start = currentPage * limit + 1;
    const end = Math.min((currentPage + 1) * limit, totalResults);
    resultCount.textContent = `(${start}-${end} of ${totalResults})`;
}

function updatePagination() {
    const totalPages = Math.ceil(totalResults / limit);
    pageInfo.textContent = `Page ${currentPage + 1} of ${totalPages}`;
    prevPageBtn.disabled = currentPage === 0;
    nextPageBtn.disabled = currentPage >= totalPages - 1;
    paginationContainer.classList.remove('hidden');
}

function showLoading(show) {
    if (show) {
        loadingIndicator.classList.remove('hidden');
        resultsContainer.classList.add('hidden');
    } else {
        loadingIndicator.classList.add('hidden');
        resultsContainer.classList.remove('hidden');
    }
}

function showError(message) {
    resultsContainer.innerHTML = `<div class="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-700 rounded-lg p-4 text-center">
        <p class="text-red-800 dark:text-red-200">${message}</p>
    </div>`;
}

// Event Listeners
function setupEventListeners() {
    applyFiltersBtn.addEventListener('click', () => {
        currentPage = 0;
        loadProteomes();
    });
    
    resetFiltersBtn.addEventListener('click', () => {
        searchInput.value = '';
        collectionSelect.value = '';
        taxonomyLevelSelect.value = '';
        taxonomyNameSelect.value = '';
        taxonomyNameSelect.disabled = true;
        buscoMetricSelect.value = '';
        buscoMinValue.value = '';
        currentPage = 0;
    });
    
    taxonomyLevelSelect.addEventListener('change', (e) => {
        loadTaxonomyNames(e.target.value);
    });
    
        selectAllColumnsBtn.addEventListener('click', () => {
                for (const section of Object.keys(COLUMN_CONFIG)) {
                    for (const item of COLUMN_CONFIG[section]) {
                        if (item.hidden) continue;
                        if (!availableColumns.includes(item.key)) continue;
                        selectedColumns.add(item.key);
                    }
                }
                // ensure mandatory stay selected
                for (const item of COLUMN_CONFIG['Proteome']) {
                    if (item.mandatory) selectedColumns.add(item.key);
                }
                renderColumnCheckboxes();
        });
    
    deselectAllColumnsBtn.addEventListener('click', () => {
        selectedColumns.clear();
        // keep mandatory columns
        for (const item of COLUMN_CONFIG['Proteome']) {
          if (item.mandatory) selectedColumns.add(item.key);
        }
        renderColumnCheckboxes();
    });
    
    applyFiltersBtn2.addEventListener('click', () => {
        currentPage = 0;
        loadProteomes();
    });
    
    resetFiltersBtn2.addEventListener('click', () => {
        searchInput.value = '';
        collectionSelect.value = '';
        taxonomyLevelSelect.value = '';
        taxonomyNameSelect.value = '';
        taxonomyNameSelect.disabled = true;
        buscoMetricSelect.value = '';
        buscoMinValue.value = '';
        currentPage = 0;
    });
    
    prevPageBtn.addEventListener('click', () => {
        if (currentPage > 0) {
            currentPage--;
            loadProteomes();
        }
    });
    
    nextPageBtn.addEventListener('click', () => {
        const totalPages = Math.ceil(totalResults / limit);
        if (currentPage < totalPages - 1) {
            currentPage++;
            loadProteomes();
        }
    });
    
    exportTsvBtn.addEventListener('click', () => exportData('tsv'));
    lifemapBtn.addEventListener('click', openLifemap);
    
    // Search on Enter
    searchInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
            currentPage = 0;
            loadProteomes();
        }
    });
}

// Initialize on load
init();
