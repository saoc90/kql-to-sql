// Enhanced file manager that handles files entirely in JavaScript
// Modified from DuckDbDemo version: uses EventTarget instead of Blazor callbacks

class FileManager extends EventTarget {
    constructor() {
        super();
        this.files = new Map(); // Store file objects directly
        this.fileHandles = new Map(); // Store File System Access API handles
        this.storageKey = 'fileManagerMetadata';
        this.fileHandlesKey = 'fileManagerHandles';
        this.maxFileSize = 500 * 1024 * 1024; // 500MB
        this.supportedTypes = ['.csv', '.gz', '.json', '.parquet', '.txt'];
        this.supportsFileSystemAccess = 'showOpenFilePicker' in window;
    }

    // Initialize the file manager (dotnetRef kept for API compat but unused)
    initialize(_dotnetRef) {
        this.loadMetadataFromStorage();
        this.loadFileHandlesFromStorage();
    }

    // Setup file input and drag/drop zones
    setupFileHandling(fileInputId, dropZoneId) {
        const fileInput = document.getElementById(fileInputId);
        const dropZone = document.getElementById(dropZoneId);

        if (fileInput) {
            fileInput.addEventListener('change', (e) => this.handleFileInput(e));
        }

        if (dropZone) {
            this.setupDropZone(dropZone);
        }

        // Add button for File System Access API if supported
        if (this.supportsFileSystemAccess) {
            this.addPersistentFileButton(dropZone);
        }
    }

    // Setup drag and drop functionality
    setupDropZone(dropZone) {
        dropZone.addEventListener('dragover', (e) => {
            e.preventDefault();
            e.stopPropagation();
            dropZone.classList.add('drag-over');
        });

        dropZone.addEventListener('dragenter', (e) => {
            e.preventDefault();
            e.stopPropagation();
            dropZone.classList.add('drag-over');
        });

        dropZone.addEventListener('dragleave', (e) => {
            e.preventDefault();
            e.stopPropagation();
            if (!dropZone.contains(e.relatedTarget)) {
                dropZone.classList.remove('drag-over');
            }
        });

        dropZone.addEventListener('drop', async (e) => {
            e.preventDefault();
            e.stopPropagation();
            dropZone.classList.remove('drag-over');
            const files = Array.from(e.dataTransfer.files);
            await this.processFiles(files);
        });
    }

    // Add persistent file access button (File System Access API)
    addPersistentFileButton(dropZone) {
        if (!dropZone) return;
        const button = document.createElement('button');
        button.textContent = 'Add Persistent Files';
        button.className = 'btn btn-outline-primary btn-sm mt-2';
        button.title = 'Files will be accessible across browser sessions (Chrome/Edge only)';
        button.addEventListener('click', () => this.selectPersistentFiles());
        dropZone.parentNode.insertBefore(button, dropZone.nextSibling);
    }

    // Select files using File System Access API
    async selectPersistentFiles() {
        try {
            const fileHandles = await window.showOpenFilePicker({
                multiple: true,
                types: [{
                    description: 'Data files',
                    accept: {
                        'text/csv': ['.csv', '.csv.gz'],
                        'application/json': ['.json'],
                        'text/plain': ['.txt'],
                        'application/octet-stream': ['.parquet']
                    }
                }]
            });

            for (const fileHandle of fileHandles) {
                await this.processPersistentFile(fileHandle);
            }
        } catch (error) {
            if (error.name !== 'AbortError') {
                console.error('Error selecting persistent files:', error);
            }
        }
    }

    // Process a file with persistent access
    async processPersistentFile(fileHandle) {
        try {
            const file = await fileHandle.getFile();
            const validation = this.validateFile(file);
            if (!validation.isValid) {
                this.notify('OnFileValidationFailed', { fileName: file.name, error: validation.error });
                return;
            }

            const fileId = this.generateFileId(file.name);
            this.files.set(fileId, file);
            this.fileHandles.set(fileId, fileHandle);

            const metadata = {
                id: fileId,
                name: file.name,
                size: file.size,
                type: file.type || this.getContentTypeFromExtension(file.name),
                uploadDate: new Date().toISOString(),
                isLoaded: false,
                hasError: false,
                tableName: null,
                rowCount: 0,
                isPersistent: true
            };

            this.saveMetadataToStorage(metadata);
            this.saveFileHandleToStorage(fileId, fileHandle);
            this.notify('OnFileAdded', metadata);
        } catch (error) {
            console.error('Error processing persistent file:', error);
            this.notify('OnFileProcessingFailed', { fileName: fileHandle.name || 'Unknown file', error: error.message });
        }
    }

    // Handle file input change
    async handleFileInput(event) {
        const files = Array.from(event.target.files);
        await this.processFiles(files);
        event.target.value = '';
    }

    // Process multiple files
    async processFiles(files) {
        if (!files || files.length === 0) return;

        try {
            this.notify('OnUploadStarted', { count: files.length });

            for (const file of files) {
                await this.processFile(file);
            }

            this.notify('OnUploadCompleted', { count: files.length, success: true });
        } catch (error) {
            console.error('Error processing files:', error);
            this.notify('OnUploadCompleted', { count: files.length, success: false, error: error.message });
        }
    }

    // Process a single file
    async processFile(file) {
        try {
            const validation = this.validateFile(file);
            if (!validation.isValid) {
                this.notify('OnFileValidationFailed', { fileName: file.name, error: validation.error });
                return;
            }

            const fileId = this.generateFileId(file.name);
            this.files.set(fileId, file);

            const metadata = {
                id: fileId,
                name: file.name,
                size: file.size,
                type: file.type || this.getContentTypeFromExtension(file.name),
                uploadDate: new Date().toISOString(),
                isLoaded: false,
                hasError: false,
                tableName: null,
                rowCount: 0
            };

            this.saveMetadataToStorage(metadata);
            this.notify('OnFileAdded', metadata);
        } catch (error) {
            console.error('Error processing file:', file.name, error);
            this.notify('OnFileProcessingFailed', { fileName: file.name, error: error.message });
        }
    }

    // Load file into DuckDB database
    async loadFileIntoDatabase(fileId) {
        try {
            let file = this.files.get(fileId);
            console.log(`Loading file with ID: ${fileId}`);

            if (!file) {
                console.log(`File not found in memory, checking file handles...`);
                file = this.fileHandles[fileId];
            }

            if (!file) {
                const fileHandle = this.fileHandles.get(fileId);
                if (fileHandle) {
                    console.log(`Found file handle for ID: ${fileId}`);
                    try {
                        const permission = await fileHandle.queryPermission({ mode: 'read' });
                        if (permission === 'granted' || permission === 'prompt') {
                            if (permission === 'prompt') {
                                await fileHandle.requestPermission({ mode: 'read' });
                            }
                            file = await fileHandle.getFile();
                            this.files.set(fileId, file);
                        } else {
                            throw new Error('Permission denied to read file');
                        }
                    } catch (permError) {
                        throw new Error(`Cannot access file: ${permError.message}. Please re-select the file.`);
                    }
                } else {
                    throw new Error('File not found in memory. Please re-upload the file.');
                }
            }

            const arrayBuffer = await file.arrayBuffer();
            const bytes = new Uint8Array(arrayBuffer);

            if (!window.db) {
                if (!window.DuckDbInterop) {
                    throw new Error('DuckDB is not available. Please refresh the page and try again.');
                }
                console.log('Initializing DuckDB for file load...');
                await window.DuckDbInterop.queryJson('SELECT 1');
            }
            if (!window.db) {
                throw new Error('DuckDB failed to initialize');
            }

            await window.db.registerFileBuffer(file.name, bytes);
            console.log(`Registered file: ${file.name}`);

            let tableName = file.name.replace(/\.[^/.]+$/, '').replace(/[^a-zA-Z0-9_]/g, '_');
            if (tableName.match(/^\d/)) {
                tableName = 'table_' + tableName;
            }

            const connection = await window.db.connect();

            try {
                await connection.query(`DROP TABLE IF EXISTS ${tableName}`);

                let sql = this.generateCreateTableSQL(file.name, file.type, tableName);
                console.log('Executing SQL:', sql);
                await connection.query(sql);

                const countResult = await connection.query(`SELECT COUNT(*) as count FROM ${tableName}`);
                const countData = countResult.toArray();
                const rowCount = Number(countData[0].count);

                const metadata = this.getMetadataFromStorage().find(m => m.id === fileId);
                if (metadata) {
                    metadata.isLoaded = true;
                    metadata.tableName = tableName;
                    metadata.rowCount = rowCount;
                    metadata.hasError = false;
                    this.saveMetadataToStorage(metadata);
                }

                console.log(`Successfully loaded ${rowCount} rows into table '${tableName}'`);

                return {
                    success: true,
                    tableName: tableName,
                    rowCount: rowCount,
                    message: `Successfully loaded ${rowCount} rows into table '${tableName}'`
                };
            } finally {
                connection.close();
            }
        } catch (error) {
            console.error('Failed to load file into database:', error);

            const metadata = this.getMetadataFromStorage().find(m => m.id === fileId);
            if (metadata) {
                metadata.hasError = true;
                metadata.isLoaded = false;
                this.saveMetadataToStorage(metadata);
            }

            return {
                success: false,
                error: error.message,
                message: `Failed to load file: ${error.message}`
            };
        }
    }

    // Load file into PGlite database
    async loadFileIntoDatabasePglite(fileId) {
        try {
            let file = this.files.get(fileId);

            if (!file) {
                // Try to re-acquire from persistent file handle (same logic as DuckDB loader)
                const fileHandle = this.fileHandles.get(fileId);
                if (fileHandle) {
                    const permission = await fileHandle.queryPermission({ mode: 'read' });
                    if (permission === 'granted' || permission === 'prompt') {
                        if (permission === 'prompt') {
                            await fileHandle.requestPermission({ mode: 'read' });
                        }
                        file = await fileHandle.getFile();
                        this.files.set(fileId, file);
                    } else {
                        throw new Error('Permission denied to read file');
                    }
                } else {
                    throw new Error('File not found in memory. Please re-upload the file.');
                }
            }

            if (!window.pg) {
                if (!window.PGliteInterop) {
                    throw new Error('PGlite is not available. Please refresh the page and try again.');
                }
                console.log('Initializing PGlite for file load...');
                await window.PGliteInterop.queryJson('SELECT 1');
            }

            const lower = file.name.toLowerCase();
            const arrayBuffer = await file.arrayBuffer();
            let bytes = new Uint8Array(arrayBuffer);
            if (lower.endsWith('.gz')) {
                if (typeof DecompressionStream !== 'undefined') {
                    const stream = new ReadableStream({ start(c){ c.enqueue(bytes); c.close(); }});
                    const ds = stream.pipeThrough(new DecompressionStream('gzip'));
                    const resp = new Response(ds);
                    const ab = await resp.arrayBuffer();
                    bytes = new Uint8Array(ab);
                } else {
                    throw new Error('Gzip not supported in this browser (no DecompressionStream)');
                }
            }
            const text = new TextDecoder().decode(bytes);
            let tableName = file.name.replace(/\.[^/.]+$/, '').replace(/[^a-zA-Z0-9_]/g, '_');
            if (/^\d/.test(tableName)) tableName = 'table_' + tableName;

            if (lower.endsWith('.parquet')) {
                throw new Error('Parquet support not implemented for PGlite backend');
            }

            const firstNewline = text.indexOf('\n');
            if (firstNewline === -1) throw new Error('Cannot detect header row');
            const headerLine = text.substring(0, firstNewline).replace(/\r$/, '');
            const headers = headerLine.split(',').map(h => h.replace(/"/g,'').trim()).filter(Boolean);
            if (!headers.length) throw new Error('No columns detected');
            const sanitized = headers.map(h => h.replace(/[^A-Za-z0-9_]/g,'_').replace(/^([0-9])/, '_$1').toLowerCase());
            const colsDef = sanitized.map(c => `"${c}" text`).join(', ');
            await window.pg.exec(`DROP TABLE IF EXISTS ${tableName}; CREATE TABLE ${tableName} (${colsDef});`);

            const blob = new Blob([bytes], { type: 'text/csv' });
            await window.pg.query(`COPY ${tableName} FROM '/dev/blob' WITH (FORMAT csv, HEADER true);`, [], { blob });
            const count = await window.pg.query(`SELECT COUNT(*) AS cnt FROM ${tableName};`);
            const rowCount = count.rows[0].cnt;

            const metadata = this.getMetadataFromStorage().find(m => m.id === fileId);
            if (metadata) {
                metadata.isLoaded = true;
                metadata.tableName = tableName;
                metadata.rowCount = rowCount;
                metadata.hasError = false;
                this.saveMetadataToStorage(metadata);
            }

            return { success: true, tableName, rowCount, message: `Loaded ${rowCount} rows into ${tableName} (PGlite)` };
        } catch (error) {
            console.error('Failed to load file into PGlite:', error);
            const metadata = this.getMetadataFromStorage().find(m => m.id === fileId);
            if (metadata) {
                metadata.hasError = true;
                metadata.isLoaded = false;
                this.saveMetadataToStorage(metadata);
            }
            return { success: false, error: error.message, message: `Failed to load file into PGlite: ${error.message}` };
        }
    }

    // Generate CREATE TABLE SQL based on file type
    generateCreateTableSQL(fileName, fileType, tableName) {
        const lowerFileName = fileName.toLowerCase();

        if (fileType === 'text/csv' || lowerFileName.endsWith('.csv') || lowerFileName.endsWith('.csv.gz')) {
            return `CREATE TABLE ${tableName} AS SELECT * FROM read_csv('${fileName}', header=true, ignore_errors=true, null_padding=true)`;
        } else if (fileType === 'application/json' || lowerFileName.endsWith('.json')) {
            return `CREATE TABLE ${tableName} AS SELECT * FROM read_json('${fileName}')`;
        } else if (lowerFileName.endsWith('.parquet')) {
            return `CREATE TABLE ${tableName} AS SELECT * FROM read_parquet('${fileName}')`;
        } else if (fileType === 'text/plain' || lowerFileName.endsWith('.txt')) {
            return `CREATE TABLE ${tableName} AS SELECT * FROM read_csv('${fileName}', header=false, ignore_errors=true, columns={'line': 'VARCHAR'})`;
        } else {
            throw new Error('Unsupported file type: ' + fileType);
        }
    }

    // Remove file from memory and storage
    removeFile(fileId) {
        this.files.delete(fileId);
        this.fileHandles.delete(fileId);
        const metadata = this.getMetadataFromStorage();
        const updatedMetadata = metadata.filter(m => m.id !== fileId);
        localStorage.setItem(this.storageKey, JSON.stringify(updatedMetadata));
        this.removeFileHandleFromStorage(fileId);
    }

    // Clear all files
    clearAllFiles() {
        this.files.clear();
        this.fileHandles.clear();
        localStorage.removeItem(this.storageKey);
        localStorage.removeItem(this.fileHandlesKey);
    }

    // Get file metadata for display
    getFileMetadata() {
        return this.getMetadataFromStorage();
    }

    // Validate file size and type
    validateFile(file) {
        if (file.size > this.maxFileSize) {
            return { isValid: false, error: `File too large: ${file.name} (max 500MB)` };
        }
        const extension = '.' + file.name.split('.').pop().toLowerCase();
        if (!this.supportedTypes.includes(extension)) {
            return { isValid: false, error: `Unsupported file type: ${file.name} (${extension})` };
        }
        return { isValid: true };
    }

    // Generate unique file ID
    generateFileId(fileName) {
        return `file_${Date.now()}_${Math.random().toString(36).substr(2, 9)}_${fileName.replace(/[^a-zA-Z0-9]/g, '_')}`;
    }

    // Get content type from file extension
    getContentTypeFromExtension(fileName) {
        const extension = fileName.toLowerCase().split('.').pop();
        const typeMap = {
            'csv': 'text/csv',
            'json': 'application/json',
            'parquet': 'application/octet-stream',
            'txt': 'text/plain'
        };
        return typeMap[extension] || 'application/octet-stream';
    }

    // Metadata storage functions
    saveMetadataToStorage(metadata) {
        const existing = this.getMetadataFromStorage();
        const index = existing.findIndex(m => m.id === metadata.id);
        if (index >= 0) {
            existing[index] = metadata;
        } else {
            existing.push(metadata);
        }
        localStorage.setItem(this.storageKey, JSON.stringify(existing));
    }

    getMetadataFromStorage() {
        try {
            const stored = localStorage.getItem(this.storageKey);
            return stored ? JSON.parse(stored) : [];
        } catch (error) {
            console.error('Error loading metadata from storage:', error);
            return [];
        }
    }

    loadMetadataFromStorage() {
        const metadata = this.getMetadataFromStorage();
        metadata.forEach(m => {
            if (!m.isPersistent) {
                m.isLoaded = false;
                m.hasError = false;
            }
        });
        if (metadata.length > 0) {
            localStorage.setItem(this.storageKey, JSON.stringify(metadata));
        }
    }

    // File Handle storage functions (for File System Access API)
    async saveFileHandleToStorage(fileId, fileHandle) {
        try {
            const handleInfo = { id: fileId, name: fileHandle.name, timestamp: Date.now() };
            const existing = this.getFileHandleReferencesFromStorage();
            existing[fileId] = handleInfo;
            localStorage.setItem(this.fileHandlesKey, JSON.stringify(existing));
        } catch (error) {
            console.warn('Could not save file handle reference:', error);
        }
    }

    loadFileHandlesFromStorage() {
        const references = this.getFileHandleReferencesFromStorage();
        if (Object.keys(references).length > 0) {
            console.log('Found persistent file references, but handles need to be re-established');
        }
    }

    getFileHandleReferencesFromStorage() {
        try {
            const stored = localStorage.getItem(this.fileHandlesKey);
            return stored ? JSON.parse(stored) : {};
        } catch (error) {
            console.error('Error loading file handle references:', error);
            return {};
        }
    }

    removeFileHandleFromStorage(fileId) {
        const references = this.getFileHandleReferencesFromStorage();
        delete references[fileId];
        localStorage.setItem(this.fileHandlesKey, JSON.stringify(references));
    }

    // Dispatch event instead of calling Blazor
    notify(eventName, data) {
        this.dispatchEvent(new CustomEvent(eventName, { detail: data }));
    }
}

// Create global instance
window.fileManager = new FileManager();

// Export functions (same API as before, minus Blazor dependency)
globalThis.FileManagerInterop = {
    initialize: (dotnetRef) => window.fileManager.initialize(dotnetRef),
    setupFileHandling: (fileInputId, dropZoneId) => window.fileManager.setupFileHandling(fileInputId, dropZoneId),
    loadFileIntoDatabase: (fileId) => window.fileManager.loadFileIntoDatabase(fileId),
    removeFile: (fileId) => window.fileManager.removeFile(fileId),
    clearAllFiles: () => window.fileManager.clearAllFiles(),
    getFileMetadata: () => window.fileManager.getFileMetadata(),
    selectPersistentFiles: () => window.fileManager.selectPersistentFiles(),
    loadFileIntoDatabasePglite: (fileId) => window.fileManager.loadFileIntoDatabasePglite(fileId)
};
