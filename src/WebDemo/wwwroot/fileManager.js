// Enhanced file manager that handles files entirely in JavaScript
// Uses OPFS for persistent file storage, with localStorage for metadata

// SQL helpers
function escapeSqlString(value) { return value.replace(/'/g, "''"); }
function quoteIdentifier(name) { return '"' + name.replace(/"/g, '""') + '"'; }

class FileManager extends EventTarget {
    constructor() {
        super();
        this.files = new Map(); // Store file objects directly
        this.fileHandles = new Map(); // Store File System Access API handles
        this.storageKey = 'fileManagerMetadata';
        this.fileHandlesKey = 'fileManagerHandles';
        this.opfsDirectoryName = 'kql-to-sql-files';
        this.maxFileSize = 500 * 1024 * 1024; // 500MB
        this.supportedTypes = ['.csv', '.gz', '.json', '.parquet', '.txt'];
        this.supportsFileSystemAccess = 'showOpenFilePicker' in window;
        this._opfsRoot = null;
        this._initialized = false; // BUG-021: guard against double initialization
    }

    // Get the OPFS directory for file storage
    async _getOpfsDir() {
        if (this._opfsRoot) return this._opfsRoot;
        try {
            if (!navigator.storage || !navigator.storage.getDirectory) return null;
            const root = await navigator.storage.getDirectory();
            this._opfsRoot = await root.getDirectoryHandle(this.opfsDirectoryName, { create: true });
            return this._opfsRoot;
        } catch (err) {
            console.warn('[FileManager] OPFS not available:', err.message);
            return null;
        }
    }

    // Save file bytes to OPFS
    async saveFileToOpfs(fileId, bytes) {
        const dir = await this._getOpfsDir();
        if (!dir) return false;
        try {
            const fileHandle = await dir.getFileHandle(fileId, { create: true });
            const writable = await fileHandle.createWritable();
            await writable.write(bytes);
            await writable.close();
            return true;
        } catch (err) {
            console.warn('[FileManager] Failed to save file to OPFS:', err.message);
            return false;
        }
    }

    // Load file bytes from OPFS
    async loadFileFromOpfs(fileId) {
        const dir = await this._getOpfsDir();
        if (!dir) return null;
        try {
            const fileHandle = await dir.getFileHandle(fileId);
            const file = await fileHandle.getFile();
            return file;
        } catch {
            return null;
        }
    }

    // Remove file from OPFS
    async removeFileFromOpfs(fileId) {
        const dir = await this._getOpfsDir();
        if (!dir) return;
        try {
            await dir.removeEntry(fileId);
        } catch {
            // File may not exist
        }
    }

    // Clear all files from OPFS
    async clearOpfsFiles() {
        const dir = await this._getOpfsDir();
        if (!dir) return;
        try {
            for await (const [name] of dir.entries()) {
                await dir.removeEntry(name);
            }
        } catch (err) {
            console.warn('[FileManager] Failed to clear OPFS files:', err.message);
        }
    }

    // Initialize the file manager (dotnetRef kept for API compat but unused)
    initialize(_dotnetRef) {
        if (this._initialized) return;
        this._initialized = true;
        this.loadMetadataFromStorage();
        this.loadFileHandlesFromStorage();
        // Fire-and-forget but catch errors to avoid unhandled rejection
        this._syncPersistedFilesOnReload().catch(err => {
            console.error('[FileManager] Sync on reload failed:', err);
        });
    }

    // On reload, sync metadata with what DuckDB actually has persisted
    async _syncPersistedFilesOnReload() {
        // Wait for DuckDB to be ready
        if (!window.db) {
            await new Promise(resolve => {
                // BUG-005: add 30-second timeout so we don't hang forever
                const timeoutId = setTimeout(() => {
                    window.removeEventListener('duckdb-ready', handler);
                    resolve();
                }, 30000);

                const handler = () => {
                    clearTimeout(timeoutId);
                    window.removeEventListener('duckdb-ready', handler);
                    resolve();
                };
                window.addEventListener('duckdb-ready', handler);
                // Also resolve if db is already set by the time we check
                if (window.db) {
                    clearTimeout(timeoutId);
                    window.removeEventListener('duckdb-ready', handler);
                    resolve();
                }
            });
        }

        const metadata = this.getMetadataFromStorage();
        if (metadata.length === 0) return;

        // BUG-005: null-check window.DuckDbInterop before calling it
        if (!window.DuckDbInterop) {
            console.warn('[FileManager] DuckDbInterop not available during sync, skipping.');
            return;
        }

        // Get actual tables from DuckDB
        let existingTables = [];
        try {
            const tablesJson = await window.DuckDbInterop.getAvailableTables();
            existingTables = JSON.parse(tablesJson);
        } catch {
            return;
        }

        // BUG-020: wrap the entire loop in try/finally to always save metadata at the end
        let changed = false;
        try {
            for (const m of metadata) {
                if (m.tableName && existingTables.includes(m.tableName)) {
                    // Table still exists in the persistent DB
                    if (!m.isLoaded) {
                        m.isLoaded = true;
                        m.hasError = false;
                        changed = true;
                    }
                    // Refresh row count
                    try {
                        const countJson = await window.DuckDbInterop.queryJson(
                            `SELECT COUNT(*) as cnt FROM ${quoteIdentifier(m.tableName)}`
                        );
                        const countData = JSON.parse(countJson);
                        if (countData.length > 0) {
                            const newCount = Number(countData[0].cnt);
                            if (m.rowCount !== newCount) {
                                m.rowCount = newCount;
                                changed = true;
                            }
                        }
                    } catch { /* ignore */ }
                } else if (m.isLoaded && m.tableName) {
                    // Table was marked as loaded but doesn't exist anymore
                    // Check if we have file data in OPFS; if so, mark for lazy reload
                    const opfsFile = await this.loadFileFromOpfs(m.id);
                    // BUG-008: don't pre-load bytes into memory; just mark isLoaded=false
                    // loadFileIntoDatabase() will lazily load from OPFS when needed
                    m.isLoaded = false;
                    m.hasError = false;
                    changed = true;
                    if (!opfsFile) {
                        console.warn(`[FileManager] OPFS file missing for ${m.name}, will need re-upload.`);
                    }
                }
            }
        } catch (err) {
            console.error('[FileManager] Error during sync loop:', err);
        } finally {
            // BUG-020: always save metadata at the end, even if an error occurred mid-loop
            if (changed) {
                localStorage.setItem(this.storageKey, JSON.stringify(metadata));
                this.notify('OnFilesRefreshed', {});
            }
        }
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

            // Also save to OPFS for persistence
            const bytes = new Uint8Array(await file.arrayBuffer());
            await this.saveFileToOpfs(fileId, bytes);

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

        let successCount = 0;
        try {
            this.notify('OnUploadStarted', { count: files.length });

            for (const file of files) {
                const ok = await this.processFile(file);
                if (ok) successCount++;
            }

            this.notify('OnUploadCompleted', {
                count: files.length,
                success: successCount > 0,
                successCount
            });
        } catch (error) {
            console.error('Error processing files:', error);
            this.notify('OnUploadCompleted', { count: files.length, success: false, successCount, error: error.message });
        }
    }

    // Process a single file. Returns true on success.
    async processFile(file) {
        try {
            const validation = this.validateFile(file);
            if (!validation.isValid) {
                this.notify('OnFileValidationFailed', { fileName: file.name, error: validation.error });
                return false;
            }

            const fileId = this.generateFileId(file.name);
            this.files.set(fileId, file);

            // Save file bytes to OPFS for persistence across reloads
            const bytes = new Uint8Array(await file.arrayBuffer());
            const savedToOpfs = await this.saveFileToOpfs(fileId, bytes);

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
                isPersistent: savedToOpfs
            };

            this.saveMetadataToStorage(metadata);
            this.notify('OnFileAdded', metadata);
            return true;
        } catch (error) {
            console.error('Error processing file:', file.name, error);
            this.notify('OnFileProcessingFailed', { fileName: file.name, error: error.message });
            return false;
        }
    }

    // Load file into DuckDB database
    // BUG-017: avoid double arrayBuffer() reads when restoring from OPFS
    async loadFileIntoDatabase(fileId) {
        try {
            let file = this.files.get(fileId);
            console.log(`Loading file with ID: ${fileId}`);

            if (!file) {
                console.log(`File not found in memory, checking OPFS...`);
                const opfsFile = await this.loadFileFromOpfs(fileId);
                if (opfsFile) {
                    // Retrieve original name from metadata
                    const metadata = this.getMetadataFromStorage().find(m => m.id === fileId);
                    const fileName = metadata?.name || opfsFile.name;
                    const fileType = metadata?.type || opfsFile.type;
                    // BUG-017: read the buffer once and reuse it for both File construction and DuckDB
                    const buffer = await opfsFile.arrayBuffer();
                    file = new File([buffer], fileName, { type: fileType });
                    this.files.set(fileId, file);
                    console.log(`Restored file from OPFS: ${fileName}`);
                }
            }

            if (!file) {
                console.log(`File not found in OPFS, checking file handles...`);
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
                    throw new Error('File not found. Please re-upload the file.');
                }
            }

            const arrayBuffer = await file.arrayBuffer();
            let bytes = new Uint8Array(arrayBuffer);
            let registeredName = file.name;

            // Decompress .gz files before registering with DuckDB
            if (file.name.toLowerCase().endsWith('.gz')) {
                try {
                    if (typeof DecompressionStream !== 'undefined') {
                        const stream = new ReadableStream({
                            start(controller) { controller.enqueue(bytes); controller.close(); }
                        });
                        const decompressed = stream.pipeThrough(new DecompressionStream('gzip'));
                        const resp = new Response(decompressed);
                        bytes = new Uint8Array(await resp.arrayBuffer());
                        registeredName = file.name.replace(/\.gz$/i, '');
                        console.log(`Decompressed ${file.name} -> ${registeredName}`);
                    }
                } catch (err) {
                    throw new Error(`Gzip decompression failed for ${file.name}: ${err.message}. Your browser may not support DecompressionStream.`);
                }
            }

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

            await window.db.registerFileBuffer(registeredName, bytes);
            console.log(`Registered file: ${registeredName}`);

            // Derive table name from the registered name (already stripped of .gz)
            let baseName = registeredName.replace(/\.[^/.]+$/, '');
            let tableName = baseName.replace(/[^a-zA-Z0-9_]/g, '_');
            if (tableName.match(/^\d/)) {
                tableName = 'table_' + tableName;
            }

            const connection = await window.db.connect();

            try {
                await connection.query(`DROP TABLE IF EXISTS ${quoteIdentifier(tableName)}`);

                let sql = this.generateCreateTableSQL(registeredName, file.type, tableName);
                console.log('Executing SQL:', sql);
                await connection.query(sql);

                const countResult = await connection.query(`SELECT COUNT(*) as count FROM ${quoteIdentifier(tableName)}`);
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

            let cachedBuffer = null; // Avoid double arrayBuffer() reads for OPFS files

            if (!file) {
                // Try to restore from OPFS
                const opfsFile = await this.loadFileFromOpfs(fileId);
                if (opfsFile) {
                    const metadata = this.getMetadataFromStorage().find(m => m.id === fileId);
                    const fileName = metadata?.name || opfsFile.name;
                    const fileType = metadata?.type || opfsFile.type;
                    cachedBuffer = await opfsFile.arrayBuffer();
                    file = new File([cachedBuffer], fileName, { type: fileType });
                    this.files.set(fileId, file);
                }
            }

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
                    throw new Error('File not found. Please re-upload the file.');
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
            const arrayBuffer = cachedBuffer || await file.arrayBuffer();
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
            // Strip compound extensions (.csv.gz, .json.gz) then single extension
            let pgliteBaseName = file.name.replace(/\.(csv|json)\.gz$/i, '').replace(/\.[^/.]+$/, '');
            let tableName = pgliteBaseName.replace(/[^a-zA-Z0-9_]/g, '_');
            if (/^\d/.test(tableName)) tableName = 'table_' + tableName;

            if (lower.endsWith('.parquet')) {
                throw new Error('Parquet support not implemented for PGlite backend');
            }
            if (lower.endsWith('.json') || lower.replace(/\.gz$/, '').endsWith('.json')) {
                throw new Error('JSON support not implemented for PGlite backend. Use the DuckDB backend for JSON files.');
            }

            const firstNewline = text.indexOf('\n');
            if (firstNewline === -1) throw new Error('Cannot detect header row');
            const headerLine = text.substring(0, firstNewline).replace(/\r$/, '');
            const headers = headerLine.split(',').map(h => h.replace(/"/g,'').trim()).filter(Boolean);
            if (!headers.length) throw new Error('No columns detected');
            const sanitized = headers.map(h => h.replace(/[^A-Za-z0-9_]/g,'_').replace(/^([0-9])/, '_$1').toLowerCase());
            const colsDef = sanitized.map(c => `${quoteIdentifier(c)} text`).join(', ');
            await window.pg.exec(`DROP TABLE IF EXISTS ${quoteIdentifier(tableName)}; CREATE TABLE ${quoteIdentifier(tableName)} (${colsDef});`);

            const blob = new Blob([bytes], { type: 'text/csv' });
            await window.pg.query(`COPY ${quoteIdentifier(tableName)} FROM '/dev/blob' WITH (FORMAT csv, HEADER true);`, [], { blob });
            const count = await window.pg.query(`SELECT COUNT(*) AS cnt FROM ${quoteIdentifier(tableName)};`);
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
    // BUG-011: support compound extensions; BUG adds .json.gz support
    generateCreateTableSQL(fileName, fileType, tableName) {
        const lowerFileName = fileName.toLowerCase();
        const quotedTable = quoteIdentifier(tableName);
        const escapedFile = escapeSqlString(fileName);

        if (lowerFileName.endsWith('.csv.gz') || fileType === 'text/csv' || lowerFileName.endsWith('.csv')) {
            return `CREATE TABLE ${quotedTable} AS SELECT * FROM read_csv('${escapedFile}', header=true, ignore_errors=true, null_padding=true)`;
        } else if (lowerFileName.endsWith('.json.gz')) {
            // BUG-011: decompress then read as JSON
            return `CREATE TABLE ${quotedTable} AS SELECT * FROM read_json('${escapedFile}')`;
        } else if (fileType === 'application/json' || lowerFileName.endsWith('.json')) {
            return `CREATE TABLE ${quotedTable} AS SELECT * FROM read_json('${escapedFile}')`;
        } else if (lowerFileName.endsWith('.parquet')) {
            return `CREATE TABLE ${quotedTable} AS SELECT * FROM read_parquet('${escapedFile}')`;
        } else if (fileType === 'text/plain' || lowerFileName.endsWith('.txt')) {
            return `CREATE TABLE ${quotedTable} AS SELECT * FROM read_csv('${escapedFile}', header=false, ignore_errors=true, columns={'line': 'VARCHAR'})`;
        } else {
            throw new Error('Unsupported file type: ' + fileType);
        }
    }

    // Remove file from memory, OPFS, and storage
    // BUG-002: make async and await OPFS operations
    async removeFile(fileId) {
        this.files.delete(fileId);
        this.fileHandles.delete(fileId);
        await this.removeFileFromOpfs(fileId);
        const metadata = this.getMetadataFromStorage();
        const updatedMetadata = metadata.filter(m => m.id !== fileId);
        localStorage.setItem(this.storageKey, JSON.stringify(updatedMetadata));
        this.removeFileHandleFromStorage(fileId);
    }

    // Clear all files
    // BUG-002: make async and await OPFS operations
    async clearAllFiles() {
        this.files.clear();
        this.fileHandles.clear();
        await this.clearOpfsFiles();
        localStorage.removeItem(this.storageKey);
        localStorage.removeItem(this.fileHandlesKey);
    }

    // Get file metadata for display
    getFileMetadata() {
        return this.getMetadataFromStorage();
    }

    // Validate file size and type
    // BUG-011: handle compound extensions like .csv.gz properly
    validateFile(file) {
        if (file.size > this.maxFileSize) {
            return { isValid: false, error: `File too large: ${file.name} (max 500MB)` };
        }
        const lowerName = file.name.toLowerCase();
        // Check compound extensions first
        const compoundExtensions = ['.csv.gz', '.json.gz'];
        const hasCompoundExt = compoundExtensions.some(ext => lowerName.endsWith(ext));
        if (!hasCompoundExt) {
            const extension = '.' + lowerName.split('.').pop();
            if (!this.supportedTypes.includes(extension)) {
                return { isValid: false, error: `Unsupported file type: ${file.name} (${extension})` };
            }
        }
        return { isValid: true };
    }

    // Generate unique file ID
    // BUG-019: use crypto.randomUUID() if available
    generateFileId(fileName) {
        const uniquePart = (typeof crypto !== 'undefined' && crypto.randomUUID)
            ? crypto.randomUUID()
            : `${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
        return `file_${uniquePart}_${fileName.replace(/[^a-zA-Z0-9]/g, '_')}`;
    }

    // Get content type from file extension
    // BUG-024: add .gz -> application/gzip and handle compound extensions
    getContentTypeFromExtension(fileName) {
        const lowerName = fileName.toLowerCase();
        // Handle compound extensions first
        if (lowerName.endsWith('.csv.gz')) return 'text/csv';
        if (lowerName.endsWith('.json.gz')) return 'application/json';
        const extension = lowerName.split('.').pop();
        const typeMap = {
            'csv': 'text/csv',
            'json': 'application/json',
            'parquet': 'application/octet-stream',
            'txt': 'text/plain',
            'gz': 'application/gzip'
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
