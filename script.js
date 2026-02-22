// Global variables
let selectedFiles = [];
let mergedData = null;
let sessionId = null;
let processingStats = null;
let sheetInfo = {};
let userData = {
    totalSheetsMerged: 0,
    todaySheetsMerged: 0,
    lastResetDate: new Date().toDateString()
};

// Processing time tracking
let mergeStartTime = null;
let mergeEndTime = null;
let actualFileSizeKB = null;

// DOM elements
const uploadArea = document.getElementById('uploadArea');
const browseButton = document.getElementById('browseButton');
const fileInput = document.getElementById('fileInput');
const fileCountEl = document.getElementById('fileCount');
const summaryFileCount = document.getElementById('summaryFileCount');
const summarySheetCount = document.getElementById('summarySheetCount');
const summaryRowCount = document.getElementById('summaryRowCount');
const summaryColumnCount = document.getElementById('summaryColumnCount');
const mergeBtn = document.getElementById('mergeBtn');
const resetBtn = document.getElementById('resetBtn');
const downloadBtn = document.getElementById('downloadBtn');
const progressBar = document.getElementById('progressBar');
const progressFill = document.getElementById('progressFill');
const sheetSelect = document.getElementById('sheetSelect');
const previewTable = document.getElementById('previewTable');
const tableHeader = document.getElementById('tableHeader');
const previewBody = document.getElementById('previewBody');
const previewInfo = document.getElementById('previewInfo');
const previewCount = document.getElementById('previewCount');
const reportModal = document.getElementById('reportModal');
const closeModal = document.getElementById('closeModal');
const closeModalBtn = document.getElementById('closeModalBtn');
const confirmDownload = document.getElementById('confirmDownload');
const modalFileCount = document.getElementById('modalFileCount');
const modalSheetCount = document.getElementById('modalSheetCount');
const modalRowCount = document.getElementById('modalRowCount');
const modalColumnCount = document.getElementById('modalColumnCount');
const modalTime = document.getElementById('modalTime');
const modalFileSize = document.getElementById('modalFileSize');
const totalSheetsCounter = document.getElementById('totalSheetsCounter');
const todaySheetsCounter = document.getElementById('todaySheetsCounter');
const currentDate = document.getElementById('currentDate');

// API endpoint
const API_BASE_URL = 'http://localhost:5000';

// Initialize
function init() {
    loadUserData();
    resetTool();
    setupEventListeners();
    updateCurrentDate();
}

// Load user data from localStorage
function loadUserData() {
    const savedData = localStorage.getItem('excelMergeToolData');
    if (savedData) {
        try {
            userData = JSON.parse(savedData);
            
            // Check if it's a new day
            const today = new Date().toDateString();
            if (userData.lastResetDate !== today) {
                userData.todaySheetsMerged = 0;
                userData.lastResetDate = today;
                saveUserData();
            }
        } catch (e) {
            console.log('Could not load user data, using defaults');
        }
    }
    
    updateStatisticsCounters();
}

// Save user data to localStorage
function saveUserData() {
    localStorage.setItem('excelMergeToolData', JSON.stringify(userData));
    updateStatisticsCounters();
}

// Update statistics counters
function updateStatisticsCounters() {
    totalSheetsCounter.textContent = userData.totalSheetsMerged.toLocaleString();
    todaySheetsCounter.textContent = userData.todaySheetsMerged.toLocaleString();
}

// Update current date display
function updateCurrentDate() {
    const now = new Date();
    const options = { 
        day: 'numeric', 
        month: 'long', 
        year: 'numeric',
        weekday: 'long'
    };
    currentDate.textContent = now.toLocaleDateString('en-IN', options);
}

// Setup event listeners
function setupEventListeners() {
    // Browse button click event
    browseButton.addEventListener('click', function(e) {
        e.preventDefault();
        e.stopPropagation();
        fileInput.click();
    });
    
    // Upload area click
    uploadArea.addEventListener('click', function(e) {
        if (e.target.closest('#browseButton')) {
            return;
        }
        fileInput.click();
    });
    
    // Drag and drop functionality
    uploadArea.addEventListener('dragover', (e) => {
        e.preventDefault();
        uploadArea.classList.add('drag-over');
    });
    
    uploadArea.addEventListener('dragleave', () => {
        uploadArea.classList.remove('drag-over');
    });
    
    uploadArea.addEventListener('drop', (e) => {
        e.preventDefault();
        uploadArea.classList.remove('drag-over');
        
        if (e.dataTransfer.files.length) {
            handleFiles(e.dataTransfer.files);
        }
    });
    
    // File input change event
    fileInput.addEventListener('change', (e) => {
        if (e.target.files && e.target.files.length > 0) {
            handleFiles(e.target.files);
        }
    });
    
    // Sheet select change event
    sheetSelect.addEventListener('change', function() {
        if (this.value === 'all') {
            updatePreview('all');
        } else if (this.value === 'source') {
            updatePreview('source');
        } else {
            const sheetKey = this.value;
            updatePreview('sheet', sheetKey);
        }
    });
    
    // Other event listeners
    mergeBtn.addEventListener('click', mergeFiles);
    resetBtn.addEventListener('click', resetTool);
    downloadBtn.addEventListener('click', showDownloadModal);
    closeModal.addEventListener('click', () => reportModal.style.display = 'none');
    closeModalBtn.addEventListener('click', () => reportModal.style.display = 'none');
    confirmDownload.addEventListener('click', downloadMergedFile);
    
    // Info tabs
    document.querySelectorAll('.info-tab').forEach(tab => {
        tab.addEventListener('click', function() {
            const tabId = this.getAttribute('data-tab');
            switchTab(tabId);
        });
    });
}

// Switch info tabs
function switchTab(tabId) {
    // Update active tab
    document.querySelectorAll('.info-tab').forEach(tab => {
        tab.classList.remove('active');
    });
    document.querySelector(`.info-tab[data-tab="${tabId}"]`).classList.add('active');
    
    // Update tab content
    document.querySelectorAll('.tab-pane').forEach(pane => {
        pane.classList.remove('active');
    });
    document.getElementById(`${tabId}-tab`).classList.add('active');
}

// Handle file selection
function handleFiles(files) {
    selectedFiles = Array.from(files);
    updateFileCount();
    
    // Reset data
    mergedData = null;
    sessionId = null;
    processingStats = null;
    sheetInfo = {};
    actualFileSizeKB = null;
    
    // Update UI
    mergeBtn.disabled = selectedFiles.length === 0;
    sheetSelect.innerHTML = '<option value="all">Consolidated View (All Sheets)</option>';
    previewInfo.textContent = selectedFiles.length + ' file(s) selected. Click "Merge Files" to process.';
    
    // Show file names in preview
    let fileListHTML = '';
    selectedFiles.forEach((file, index) => {
        const fileSize = (file.size / 1024).toFixed(1);
        const fileType = file.name.split('.').pop().toUpperCase();
        fileListHTML += `
            <div style="display: flex; align-items: center; gap: 0.8rem; padding: 0.8rem; background: white; border-radius: 6px; margin-bottom: 0.5rem; border-left: 4px solid #1E3C72; box-shadow: 0 2px 4px rgba(0,0,0,0.05);">
                <i class="fas fa-file-excel" style="color: #1E3C72; font-size: 1.2rem;"></i>
                <div style="flex: 1;">
                    <div style="font-weight: 600; font-family: 'Segoe UI', sans-serif; color: #333;">${file.name}</div>
                    <div style="display: flex; gap: 1rem; font-size: 0.8rem; color: #777; margin-top: 0.2rem;">
                        <span>${fileSize} KB</span>
                        <span>•</span>
                        <span>${fileType} file</span>
                    </div>
                </div>
                <div style="background: #1E3C72; color: white; width: 24px; height: 24px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 0.8rem;">
                    ${index + 1}
                </div>
            </div>
        `;
    });
    
    previewBody.innerHTML = `
        <tr>
            <td colspan="100" style="padding: 2rem;">
                <div style="display: flex; align-items: center; gap: 1rem; margin-bottom: 1.5rem;">
                    <div style="background: #1E3C72; color: white; width: 48px; height: 48px; border-radius: 10px; display: flex; align-items: center; justify-content: center;">
                        <i class="fas fa-file-excel" style="font-size: 1.5rem;"></i>
                    </div>
                    <div>
                        <h3 style="color: #1E3C72; margin: 0;">Selected Files (${selectedFiles.length})</h3>
                        <p style="color: #777; margin: 0.3rem 0 0 0;">Supports .xlsx, .xls, .xlsm, and .csv files</p>
                    </div>
                </div>
                <div style="background: #f9fbff; padding: 1.5rem; border-radius: 8px; max-height: 300px; overflow-y: auto; border: 1px dashed #d0ddff;">
                    ${fileListHTML}
                </div>
                <div style="margin-top: 1.5rem; padding: 1.2rem; background: #e7f4ff; border-radius: 8px; border-left: 4px solid #4a7dff;">
                    <div style="display: flex; align-items: flex-start; gap: 0.8rem; color: #1E3C72;">
                        <i class="fas fa-info-circle" style="font-size: 1.2rem; margin-top: 0.1rem;"></i>
                        <div>
                            <div style="font-weight: 600; margin-bottom: 0.3rem;">Ready to Merge</div>
                            <div style="font-size: 0.95rem;">
                                <div style="margin-bottom: 0.3rem;">• Simple and robust data extraction</div>
                                <div style="margin-bottom: 0.3rem;">• Preserves merged cells and headers</div>
                                <div>• Dark blue headers with clean data rows</div>
                            </div>
                        </div>
                    </div>
                </div>
            </td>
        </tr>
    `;
    
    previewCount.textContent = 'Showing 0 rows';
    fileInput.value = '';
}

function updateFileCount() {
    fileCountEl.textContent = selectedFiles.length;
    summaryFileCount.textContent = selectedFiles.length;
}

async function mergeFiles() {
    if (selectedFiles.length === 0) {
        showNotification('Please select at least one Excel file to merge.', 'error');
        return;
    }
    
    mergeStartTime = Date.now();
    
    // Update UI for processing
    mergeBtn.innerHTML = '<div class="loading"></div> Processing...';
    mergeBtn.disabled = true;
    downloadBtn.disabled = true;
    progressBar.style.display = 'block';
    progressFill.style.width = '10%';
    
    // Show processing status
    previewInfo.textContent = 'Processing ' + selectedFiles.length + ' file(s)...';
    
    try {
        // Create FormData to send files to Python backend
        const formData = new FormData();
        selectedFiles.forEach((file, index) => {
            formData.append('files', file);
        });
        
        // Send to Python backend
        progressFill.style.width = '30%';
        const response = await fetch(API_BASE_URL + '/merge', {
            method: 'POST',
            body: formData
        });
        
        progressFill.style.width = '70%';
        
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Server error');
        }
        
        const result = await response.json();
        progressFill.style.width = '100%';
        
        if (result.error) {
            throw new Error(result.error);
        }
        
        if (!result.success) {
            throw new Error('Merge operation failed');
        }
        
        // Store merged data
        mergedData = result.data;
        sessionId = result.download_id;
        processingStats = result.stats;
        sheetInfo = result.sheet_info || {};
        
        // Calculate processing time
        mergeEndTime = Date.now();
        
        // Update summary
        summarySheetCount.textContent = result.stats.tables || result.stats.sheets || 0;
        summaryRowCount.textContent = result.stats.rows || 0;
        summaryColumnCount.textContent = result.stats.columns || 0;
        
        // Update sheet dropdown with proper sheet options
        updateSheetDropdown();
        
        // Update preview
        updatePreview('all');
        
        // Enable download button
        downloadBtn.disabled = false;
        
        // Calculate processing time for display
        const processingTime = ((mergeEndTime - mergeStartTime) / 1000).toFixed(2);
        
        // Reset UI
        mergeBtn.innerHTML = '<i class="fas fa-cogs"></i> Merge Files';
        mergeBtn.disabled = false;
        progressBar.style.display = 'none';
        
        showNotification('Successfully merged ' + selectedFiles.length + ' file(s) with ' + (result.stats.tables || result.stats.sheets || 0) + ' sheet(s) in ' + processingTime + 's', 'success');
        
    } catch (error) {
        console.error('Error merging files:', error);
        showNotification('Error: ' + error.message, 'error');
        
        // Reset UI
        mergeBtn.innerHTML = '<i class="fas fa-cogs"></i> Merge Files';
        mergeBtn.disabled = false;
        progressBar.style.display = 'none';
        previewInfo.textContent = 'Error: ' + error.message;
    }
}

function updateSheetDropdown() {
    sheetSelect.innerHTML = '';
    
    // Add consolidated view option
    const allOption = document.createElement('option');
    allOption.value = 'all';
    allOption.textContent = 'Consolidated View (All Sheets)';
    sheetSelect.appendChild(allOption);
    
    // Add individual sheets
    for (const [key, info] of Object.entries(sheetInfo)) {
        const option = document.createElement('option');
        option.value = key; // Store the key to identify the sheet
        option.textContent = `${info.filename} - ${info.sheet_name}`;
        sheetSelect.appendChild(option);
    }
}

function updatePreview(viewType, sheetKey = null) {
    if (!mergedData || !mergedData.consolidated || mergedData.consolidated.length === 0) {
        previewBody.innerHTML = `
            <tr>
                <td colspan="100" style="text-align: center; padding: 4rem; color: #777;">
                    <div style="background: #f9fbff; padding: 3rem; border-radius: 12px; border: 2px dashed #d0ddff;">
                        <i class="fas fa-file-excel" style="font-size: 4rem; margin-bottom: 1.5rem; display: block; color: #1E3C72;"></i>
                        <h3 style="margin-bottom: 0.5rem; color: #1E3C72; font-size: 1.5rem;">No Data to Display</h3>
                        <p style="color: #777; max-width: 400px; margin: 0 auto;">Upload Excel files and click "Merge Files" to see the preview here</p>
                    </div>
                </td>
            </tr>
        `;
        previewInfo.textContent = 'No data loaded';
        previewCount.textContent = 'Showing 0 rows';
        return;
    }
    
    // Get headers (first row of consolidated data contains column names)
    const headers = mergedData.consolidated[0];
    const allDataRows = mergedData.consolidated.slice(1);
    
    // Find indices of source columns
    const sourceFileIndex = headers.indexOf('Source_File');
    const sourceSheetIndex = headers.indexOf('Source_Sheet');
    
    let rowsToDisplay = [];
    let displayInfo = '';
    let totalRowsInView = 0;
    
    if (viewType === 'all') {
        // Show all rows
        rowsToDisplay = allDataRows;
        displayInfo = 'Consolidated view of all sheets (' + Object.keys(sheetInfo).length + ' sheets)';
        totalRowsInView = allDataRows.length;
    } else if (viewType === 'sheet' && sheetKey && sheetInfo[sheetKey]) {
        // Show only rows from specific sheet
        const sheet = sheetInfo[sheetKey];
        rowsToDisplay = allDataRows.filter(row => {
            return row[sourceFileIndex] === sheet.filename && 
                   row[sourceSheetIndex] === sheet.sheet_name;
        });
        displayInfo = `Sheet: ${sheet.filename} - ${sheet.sheet_name}`;
        totalRowsInView = sheet.row_count || rowsToDisplay.length;
    } else if (viewType === 'source') {
        // Show only source columns
        return showSourceColumnsOnly();
    }
    
    // Update table headers
    tableHeader.innerHTML = '';
    let headerRow = '<tr>';
    
    headers.forEach((header, index) => {
        // Clean header for display
        let headerText = header || 'Column ' + (index + 1);
        
        // Replace hierarchy indicators with arrows for better readability
        headerText = headerText.replace(/__/g, ' → ');
        headerText = headerText.replace(/_/g, ' ');
        
        // Special styling for source columns
        if (header === 'Source_File' || header === 'Source_Sheet') {
            headerRow += '<th style="background: linear-gradient(135deg, #1E3C72 0%, #2A5298 100%); color: white; font-weight: bold; min-width: 150px; border-right: 1px solid white; position: sticky; left: ' + (index * 150) + 'px; z-index: 11; padding: 0.9rem; border-bottom: 2px solid white;">' +
                '<div style="display: flex; align-items: center; gap: 0.5rem;">' +
                '<i class="fas fa-database"></i>' +
                '<span>' + headerText + '</span>' +
                '</div>' +
                '</th>';
        } else {
            headerRow += '<th style="background: linear-gradient(135deg, #1E3C72 0%, #2A5298 100%); color: white; font-weight: 600; min-width: 120px; border-right: 1px solid rgba(255,255,255,0.2); padding: 0.9rem; border-bottom: 2px solid rgba(255,255,255,0.2);">' +
                headerText +
                '</th>';
        }
    });
    
    headerRow += '</tr>';
    tableHeader.innerHTML = headerRow;
    
    // Update table body with data
    previewBody.innerHTML = '';
    
    // Show only first 50 rows for performance
    const rowsToShow = rowsToDisplay.slice(0, 50);
    
    rowsToShow.forEach((row, rowIndex) => {
        let rowHTML = '<tr>';
        
        headers.forEach((header, colIndex) => {
            let value = row[colIndex] !== undefined ? row[colIndex] : '';
            
            // Format numbers with commas
            if (typeof value === 'number' && !isNaN(value)) {
                // Format with commas
                value = value.toLocaleString('en-IN', {
                    minimumFractionDigits: 0,
                    maximumFractionDigits: 2
                });
                
                // Add color for zero values
                if (parseFloat(value.replace(/,/g, '')) === 0) {
                    value = '<span style="color: #999; font-style: italic;">' + value + '</span>';
                }
            }
            
            // Convert null/undefined to empty string
            if (value === null || value === undefined || value === '') {
                value = '<span style="color: #ccc;">-</span>';
            }
            
            // Special styling for source columns (sticky)
            if (header === 'Source_File' || header === 'Source_Sheet') {
                rowHTML += '<td style="font-weight: 600; background-color: #f0f5ff; color: #1E3C72; border-right: 2px solid #d0ddff; position: sticky; left: ' + (colIndex * 150) + 'px; z-index: 10; padding: 0.8rem; font-family: \'Segoe UI\', sans-serif;">' +
                    value +
                    '</td>';
            } else {
                // Alternating row colors
                const bgColor = rowIndex % 2 === 0 ? '#FFFFFF' : '#F8F9FA';
                rowHTML += '<td style="background-color: ' + bgColor + '; border-right: 1px solid #F1F3F5; padding: 0.8rem;">' +
                    value +
                    '</td>';
            }
        });
        
        rowHTML += '</tr>';
        previewBody.innerHTML += rowHTML;
    });
    
    // Add info row if showing limited data
    if (totalRowsInView > 50) {
        previewBody.innerHTML +=
            '<tr>' +
            '<td colspan="' + headers.length + '" style="text-align: center; padding: 1.2rem; background: #e7f4ff; color: #1E3C72; font-weight: 600;">' +
            '<div style="display: flex; align-items: center; justify-content: center; gap: 0.8rem;">' +
            '<i class="fas fa-info-circle"></i>' +
            '<span>Showing first 50 rows only. Full data will be included in the downloaded file.</span>' +
            '</div>' +
            '</td>' +
            '</tr>';
    }
    
    previewInfo.textContent = displayInfo;
    previewCount.textContent = 'Showing ' + Math.min(50, totalRowsInView) + ' of ' + totalRowsInView + ' total rows';
}

function showSourceColumnsOnly() {
    if (!mergedData || !mergedData.consolidated || mergedData.consolidated.length === 0) {
        return;
    }
    
    const headers = mergedData.consolidated[0];
    const rows = mergedData.consolidated.slice(1, 51);
    const totalRows = mergedData.consolidated.length - 1;
    
    // Filter to only source columns
    const sourceHeaders = headers.filter(h => h === 'Source_File' || h === 'Source_Sheet');
    const sourceIndices = headers.map((h, i) => h === 'Source_File' || h === 'Source_Sheet' ? i : -1).filter(i => i !== -1);
    
    // Update headers
    tableHeader.innerHTML = '';
    let headerRow = '<tr>';
    sourceHeaders.forEach((header, index) => {
        headerRow += '<th style="background: linear-gradient(135deg, #1E3C72 0%, #2A5298 100%); color: white; font-weight: bold; min-width: 200px; border-right: 1px solid white; position: sticky; left: ' + (index * 200) + 'px; z-index: 11; padding: 0.9rem;">' +
            '<div style="display: flex; align-items: center; gap: 0.5rem;">' +
            '<i class="fas fa-database"></i>' +
            '<span>' + header + '</span>' +
            '</div>' +
            '</th>';
    });
    headerRow += '</tr>';
    tableHeader.innerHTML = headerRow;
    
    // Update rows
    previewBody.innerHTML = '';
    rows.forEach((row, rowIndex) => {
        let rowHTML = '<tr>';
        sourceIndices.forEach((colIndex, idx) => {
            let value = row[colIndex] !== undefined ? row[colIndex] : '';
            rowHTML += '<td style="font-weight: 600; background-color: ' + (rowIndex % 2 === 0 ? '#FFFFFF' : '#F8F9FA') + '; color: #1E3C72; border-right: 2px solid #d0ddff; position: sticky; left: ' + (idx * 200) + 'px; z-index: 10; padding: 0.8rem; font-family: \'Segoe UI\', sans-serif;">' +
                (value || '<span style="color: #ccc;">-</span>') +
                '</td>';
        });
        rowHTML += '</tr>';
        previewBody.innerHTML += rowHTML;
    });
    
    // Add info row if showing limited data
    if (totalRows > 50) {
        previewBody.innerHTML +=
            '<tr>' +
            '<td colspan="' + sourceHeaders.length + '" style="text-align: center; padding: 1.2rem; background: #e7f4ff; color: #1E3C72; font-weight: 600;">' +
            '<div style="display: flex; align-items: center; justify-content: center; gap: 0.8rem;">' +
            '<i class="fas fa-info-circle"></i>' +
            '<span>Showing first 50 rows only. Full data will be included in the downloaded file.</span>' +
            '</div>' +
            '</td>' +
            '</tr>';
    }
    
    previewInfo.textContent = 'Source columns only view';
    previewCount.textContent = 'Showing ' + Math.min(50, rows.length) + ' of ' + totalRows + ' total rows';
}

function showDownloadModal() {
    if (!mergedData || !sessionId || !processingStats) {
        showNotification('No data to download. Please merge files first.', 'error');
        return;
    }
    
    // Use actual file size if available, otherwise calculate accurate estimate
    let fileSizeDisplay = '';
    if (actualFileSizeKB) {
        // Show actual file size from previous download
        if (actualFileSizeKB >= 1024) {
            fileSizeDisplay = (actualFileSizeKB / 1024).toFixed(2) + ' MB';
        } else {
            fileSizeDisplay = actualFileSizeKB.toFixed(0) + ' KB';
        }
    } else {
        // Calculate more accurate file size estimate for merged Excel
        const estimatedSizeKB = calculateAccurateFileSize();
        if (estimatedSizeKB >= 1024) {
            fileSizeDisplay = (estimatedSizeKB / 1024).toFixed(2) + ' MB';
        } else {
            fileSizeDisplay = estimatedSizeKB.toFixed(0) + ' KB';
        }
    }
    
    // Calculate processing time
    const processingTime = mergeStartTime && mergeEndTime ? 
        ((mergeEndTime - mergeStartTime) / 1000).toFixed(2) + 's' : '0s';
    
    // Update modal with current data
    modalFileCount.textContent = selectedFiles.length;
    modalSheetCount.textContent = processingStats.tables || processingStats.sheets || 0;
    modalRowCount.textContent = processingStats.rows || 0;
    modalColumnCount.textContent = processingStats.columns || 0;
    modalTime.textContent = processingTime;
    modalFileSize.textContent = fileSizeDisplay;
    
    reportModal.style.display = 'flex';
}

function calculateAccurateFileSize() {
    if (!processingStats || !mergedData) return 0;
    
    const rows = processingStats.rows || 0;
    const columns = processingStats.columns || 0;
    
    // More accurate estimation for Excel files:
    // Excel uses compression (ZIP), so actual file size is much smaller than raw data
    // Typical compression ratio for Excel is about 10:1 for text data
    
    // Estimate based on actual data characteristics
    let totalDataSize = 0;
    
    if (mergedData.consolidated && mergedData.consolidated.length > 1) {
        // Sample first 100 rows to estimate data density
        const sampleRows = Math.min(100, mergedData.consolidated.length - 1);
        let totalChars = 0;
        let numericCells = 0;
        
        for (let i = 1; i <= sampleRows; i++) {
            const row = mergedData.consolidated[i];
            if (row) {
                for (let j = 0; j < Math.min(columns, row.length); j++) {
                    const cell = row[j];
                    if (cell !== null && cell !== undefined && cell !== '') {
                        if (typeof cell === 'number') {
                            numericCells++;
                            totalChars += 8; // Average number size
                        } else {
                            totalChars += String(cell).length;
                        }
                    }
                }
            }
        }
        
        // Calculate average bytes per cell
        const avgBytesPerCell = (totalChars * 2) / (sampleRows * Math.min(columns, mergedData.consolidated[1]?.length || columns));
        
        // Excel compression factor (typically 10:1 for text, better for numbers)
        const compressionFactor = 0.15; // 15% of raw size
        
        // Estimate total size
        totalDataSize = rows * columns * avgBytesPerCell * compressionFactor;
    } else {
        // Fallback estimation
        totalDataSize = rows * columns * 20 * 0.15; // 20 bytes per cell raw, 15% compressed
    }
    
    // Add Excel overhead (file structure, styles, etc.)
    const excelOverhead = 50 * 1024; // 50KB overhead for Excel file structure
    
    return Math.max(10, (totalDataSize + excelOverhead) / 1024); // Return in KB, minimum 10KB
}

async function downloadMergedFile() {
    if (!sessionId) {
        showNotification('No merged file available for download.', 'error');
        return;
    }
    
    try {
        // Update button to show downloading
        confirmDownload.innerHTML = '<div class="loading"></div> Downloading...';
        confirmDownload.disabled = true;
        
        // Download the merged file from Python backend
        const response = await fetch(API_BASE_URL + '/download/' + sessionId);
        
        if (!response.ok) {
            throw new Error('Download failed');
        }
        
        const blob = await response.blob();
        const actualSizeKB = blob.size / 1024;
        
        // Store actual file size for future reference
        actualFileSizeKB = actualSizeKB;
        
        const url = URL.createObjectURL(blob);
        
        const a = document.createElement('a');
        a.href = url;
        a.download = 'Merged_Excel_' + new Date().toISOString().slice(0, 10) + '.xlsx';
        document.body.appendChild(a);
        a.click();
        
        setTimeout(() => {
            document.body.removeChild(a);
            URL.revokeObjectURL(url);
        }, 100);
        
        // Update statistics
        userData.totalSheetsMerged += (processingStats.tables || processingStats.sheets || 0);
        userData.todaySheetsMerged += (processingStats.tables || processingStats.sheets || 0);
        saveUserData();
        
        // Update modal with actual file size
        let actualSizeDisplay = '';
        if (actualSizeKB >= 1024) {
            actualSizeDisplay = (actualSizeKB / 1024).toFixed(2) + ' MB';
        } else {
            actualSizeDisplay = actualSizeKB.toFixed(0) + ' KB';
        }
        modalFileSize.textContent = actualSizeDisplay;
        
        // Reset button
        confirmDownload.innerHTML = '<i class="fas fa-download"></i> Download Merged File';
        confirmDownload.disabled = false;
        
        // Close modal after a short delay
        setTimeout(() => {
            reportModal.style.display = 'none';
            showNotification('Merged Excel file (' + actualSizeDisplay + ') downloaded successfully!', 'success');
        }, 500);
        
    } catch (error) {
        console.error('Error downloading file:', error);
        
        // Reset button
        confirmDownload.innerHTML = '<i class="fas fa-download"></i> Download Merged File';
        confirmDownload.disabled = false;
        
        showNotification('Download error: ' + error.message, 'error');
    }
}

function resetTool() {
    selectedFiles = [];
    mergedData = null;
    sessionId = null;
    processingStats = null;
    sheetInfo = {};
    mergeStartTime = null;
    mergeEndTime = null;
    actualFileSizeKB = null;
    
    // Reset UI
    updateFileCount();
    summarySheetCount.textContent = '0';
    summaryRowCount.textContent = '0';
    summaryColumnCount.textContent = '0';
    
    mergeBtn.disabled = true;
    downloadBtn.disabled = true;
    progressBar.style.display = 'none';
    
    sheetSelect.innerHTML = '<option value="all">Consolidated View (All Sheets)</option>';
    
    previewBody.innerHTML = `
        <tr>
            <td colspan="100" style="text-align: center; padding: 4rem; color: #777;">
                <div style="background: #f9fbff; padding: 3rem; border-radius: 12px; border: 2px dashed #d0ddff;">
                    <i class="fas fa-file-excel" style="font-size: 4rem; margin-bottom: 1.5rem; display: block; color: #1E3C72;"></i>
                    <h3 style="margin-bottom: 0.5rem; color: #1E3C72; font-size: 1.5rem;">Excel Merge Tool Ready</h3>
                    <p style="color: #777; max-width: 400px; margin: 0 auto;">Drag & drop Excel files or click "Browse Files" to begin merging</p>
                </div>
            </td>
        </tr>
    `;
    
    previewInfo.textContent = 'No data loaded. Please upload Excel files to begin.';
    previewCount.textContent = 'Showing 0 rows';
    
    fileInput.value = '';
    
    showNotification('Tool has been reset successfully.', 'success');
}

function showNotification(message, type = 'info') {
    // Remove existing notifications
    document.querySelectorAll('.notification').forEach(n => n.remove());
    
    // Create notification element
    const notification = document.createElement('div');
    notification.className = 'notification';
    notification.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        padding: 1rem 1.5rem;
        border-radius: 8px;
        color: white;
        font-weight: 600;
        z-index: 9999;
        display: flex;
        align-items: center;
        gap: 0.8rem;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
        animation: slideIn 0.3s ease;
        max-width: 400px;
        line-height: 1.5;
    `;
    
    // Set color based on type
    if (type === 'success') {
        notification.style.background = 'linear-gradient(135deg, #28a745 0%, #218838 100%)';
        notification.style.borderLeft = '4px solid #155724';
    } else if (type === 'error') {
        notification.style.background = 'linear-gradient(135deg, #dc3545 0%, #c82333 100%)';
        notification.style.borderLeft = '4px solid #721c24';
    } else {
        notification.style.background = 'linear-gradient(135deg, #4a7dff 0%, #2a5298 100%)';
        notification.style.borderLeft = '4px solid #1E3C72';
    }
    
    // Add icon based on type
    let icon = 'info-circle';
    if (type === 'success') icon = 'check-circle';
    if (type === 'error') icon = 'exclamation-circle';
    
    notification.innerHTML =
        '<i class="fas fa-' + icon + '" style="font-size: 1.2rem;"></i>' +
        '<span>' + message + '</span>';
    
    document.body.appendChild(notification);
    
    // Remove notification after 5 seconds
    setTimeout(() => {
        notification.style.animation = 'slideOut 0.3s ease';
        setTimeout(() => {
            if (notification.parentNode) {
                notification.parentNode.removeChild(notification);
            }
        }, 300);
    }, 5000);
    
    // Add CSS for animations if not exists
    if (!document.querySelector('#notification-styles')) {
        const style = document.createElement('style');
        style.id = 'notification-styles';
        style.textContent = `
            @keyframes slideIn {
                from { transform: translateX(100%); opacity: 0; }
                to { transform: translateX(0); opacity: 1; }
            }
            @keyframes slideOut {
                from { transform: translateX(0); opacity: 1; }
                to { transform: translateX(100%); opacity: 0; }
            }
        `;
        document.head.appendChild(style);
    }
}

// Initialize the application
init();