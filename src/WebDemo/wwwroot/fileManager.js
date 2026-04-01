// Enhanced file manager that handles files entirely in JavaScript
// Uses OPFS (Origin Private File System) for persistence across page reloads

class FileManager extends EventTarget {
    constructor() {
        super();
        this.files = new Map(); // In-memory file objects
        this.storageKey = 'fileManagerMetadata';
        this.opfsDir = null;
        this.maxFileSize = 500 * 1024 * 1024; // 500MB
        this.supportedTypes = ['.csv', '.gz', '.json', '.parquet', '.txt'];
        this.supportsFileSystemAccess = 'showOpenFilePicker' in window;
        this.supportsOPFS = 'storage' in navigator && 'getDirectory' in navigator.storage;
    }

    // ── OPFS helpers ──────────────────────────────────────────────────

    async getOPFSDir() {
        if (!this.opfsDir) {
            const root = await navigator.storage.getDirectory();
            this.opfsDir = await root.getDirectoryHandle('fileManager', { create: true });
        }
        return this.opfsDir;
    }

    async saveToOPFS(fileId, file) {
        if (!this.supportsOPFS) return;
        try {
            const dir = await this.getOPFSDir();
            const handle = await dir.getFileHandle(fileId, { create: true });
            const writable = await handle.createWritable();
            await writable.write(await file.arrayBuffer());
            await writable.close();
        } catch (err) {
            console.warn('OPFS save failed:', err);
        }
    }

    async loadFromOPFS(fileId, originalName, originalType) {
        if (!this.supportsOPFS) return null;
        try {
            const dir = await this.getOPFSDir();
            const handle = await dir.getFileHandle(fileId);
            const opfsFile = await handle.getFile();
            // Re-create File with the original name so DuckDB sees the right filename
            return new File([await opfsFile.arrayBuffer()], originalName, { type: originalType });
        } catch {
            return null;
        }
    }

    async removeFromOPFS(fileId) {
        if (!this.supportsOPFS) return;
        try {
            const dir = await this.getOPFSDir();
            await dir.removeEntry(fileId);
        } catch { /* ignore */ }
    }

    async clearOPFS() {
        if (!this.supportsOPFS) return;
        try {
            const root = await navigator.storage.getDirectory();
            await root.removeEntry('fileManager', { recursive: true });
            this.opfsDir = null;
        } catch { /* ignore */ }
    }

    // ── Initialization ────────────────────────────────────────────────

    async initialize() {
        // Restore files from OPFS
        const metadata = this.getMetadataFromStorage();
        const valid = [];

        for (const m of metadata) {
            const file = await this.loadFromOPFS(m.id, m.name, m.type);
            if (file) {
                this.files.set(m.id, file);
                // Tables don't survive page refresh — mark for re-loading
                m.isLoaded = false;
                m.hasError = false;
                valid.push(m);
            }
            // else: file gone from OPFS → drop stale metadata
        }

        localStorage.setItem(this.storageKey, JSON.stringify(valid));

        // Notify UI about each restored file so it can auto-load
        for (const m of valid) {
            this.notify('OnFileRestored', m);
        }
    }

    // ── File input / drag-drop setup ──────────────────────────────────

    setupFileHandling(fileInputId, dropZoneId) {
        const fileInput = document.getElementById(fileInputId);
        const dropZone = document.getElementById(dropZoneId);

        if (fileInput) {
            fileInput.addEventListener('change', (e) => this.handleFileInput(e));
        }

        if (dropZone) {
            this.setupDropZone(dropZone);
        }

        // When File System Access API is supported, make the browse button
        // use the nicer native picker (files still get persisted via OPFS).
        if (this.supportsFileSystemAccess && dropZone) {
            const browseBtn = dropZone.querySelector('#btn-browse-files');
            if (browseBtn) {
                browseBtn.addEventListener('click', (e) => {
                    e.preventDefault();
                    e.stopImmediatePropagation();
                    this.selectFilesViaAccessAPI();
                });
            }
        }
    }

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

    // ── File selection via File System Access API ──────────────────────

    async selectFilesViaAccessAPI() {
        try {
            const handles = await window.showOpenFilePicker({
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

            const files = [];
            for (const h of handles) {
                files.push(await h.getFile());
            }
            await this.processFiles(files);
        } catch (error) {
            if (error.name !== 'AbortError') {
                console.error('Error selecting files:', error);
            }
        }
    }

    // ── File processing ───────────────────────────────────────────────

    async handleFileInput(event) {
        const files = Array.from(event.target.files);
        await this.processFiles(files);
        event.target.value = '';
    }

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

    async processFile(file) {
        try {
            const validation = this.validateFile(file);
            if (!validation.isValid) {
                this.notify('OnFileValidationFailed', { fileName: file.name, error: validation.error });
                return;
            }

            const fileId = this.generateFileId(file.name);
            this.files.set(fileId, file);

            // Persist to OPFS
            await this.saveToOPFS(fileId, file);

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

    // ── Database loading (DuckDB) ─────────────────────────────────────

    async loadFileIntoDatabase(fileId) {
        try {
            const file = this.files.get(fileId);
            if (!file) {
                throw new Error('File not found in memory. Please re-upload the file.');
            }

            const arrayBuffer = await file.arrayBuffer();
            const bytes = new Uint8Array(arrayBuffer);

            if (!window.db) {
                if (!window.DuckDbInterop) {
                    throw new Error('DuckDB is not available. Please refresh the page and try again.');
                }
                await window.DuckDbInterop.queryJson('SELECT 1');
            }
            if (!window.db) {
                throw new Error('DuckDB failed to initialize');
            }

            await window.db.registerFileBuffer(file.name, bytes);

            let tableName = file.name.replace(/\.[^/.]+$/, '').replace(/[^a-zA-Z0-9_]/g, '_');
            if (tableName.match(/^\d/)) {
                tableName = 'table_' + tableName;
            }

            const connection = await window.db.connect();
            try {
                await connection.query(`DROP TABLE IF EXISTS ${tableName}`);
                const sql = this.generateCreateTableSQL(file.name, file.type, tableName);
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

                return { success: true, tableName, rowCount, message: `Loaded ${rowCount} rows into '${tableName}'` };
            } finally {
                connection.close();
            }
        } catch (error) {
            console.error('Failed to load file into DuckDB:', error);

            const metadata = this.getMetadataFromStorage().find(m => m.id === fileId);
            if (metadata) {
                metadata.hasError = true;
                metadata.isLoaded = false;
                this.saveMetadataToStorage(metadata);
            }

            return { success: false, error: error.message, message: `Failed to load file: ${error.message}` };
        }
    }

    // ── Database loading (PGlite) ─────────────────────────────────────

    async loadFileIntoDatabasePglite(fileId) {
        try {
            const file = this.files.get(fileId);
            if (!file) {
                throw new Error('File not found in memory. Please re-upload the file.');
            }

            if (!window.pg) {
                if (!window.PGliteInterop) {
                    throw new Error('PGlite is not available. Please refresh the page and try again.');
                }
                await window.PGliteInterop.queryJson('SELECT 1');
            }

            const lower = file.name.toLowerCase();
            const arrayBuffer = await file.arrayBuffer();
            let bytes = new Uint8Array(arrayBuffer);
            if (lower.endsWith('.gz')) {
                if (typeof DecompressionStream !== 'undefined') {
                    const stream = new ReadableStream({ start(c) { c.enqueue(bytes); c.close(); } });
                    const ds = stream.pipeThrough(new DecompressionStream('gzip'));
                    const resp = new Response(ds);
                    bytes = new Uint8Array(await resp.arrayBuffer());
                } else {
                    throw new Error('Gzip not supported in this browser');
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
            const headers = headerLine.split(',').map(h => h.replace(/"/g, '').trim()).filter(Boolean);
            if (!headers.length) throw new Error('No columns detected');
            const sanitized = headers.map(h => h.replace(/[^A-Za-z0-9_]/g, '_').replace(/^([0-9])/, '_$1').toLowerCase());
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

    // ── SQL generation ────────────────────────────────────────────────

    generateCreateTableSQL(fileName, fileType, tableName) {
        const lower = fileName.toLowerCase();
        if (fileType === 'text/csv' || lower.endsWith('.csv') || lower.endsWith('.csv.gz')) {
            return `CREATE TABLE ${tableName} AS SELECT * FROM read_csv('${fileName}', header=true, ignore_errors=true, null_padding=true)`;
        } else if (fileType === 'application/json' || lower.endsWith('.json')) {
            return `CREATE TABLE ${tableName} AS SELECT * FROM read_json('${fileName}')`;
        } else if (lower.endsWith('.parquet')) {
            return `CREATE TABLE ${tableName} AS SELECT * FROM read_parquet('${fileName}')`;
        } else if (fileType === 'text/plain' || lower.endsWith('.txt')) {
            return `CREATE TABLE ${tableName} AS SELECT * FROM read_csv('${fileName}', header=false, ignore_errors=true, columns={'line': 'VARCHAR'})`;
        }
        throw new Error('Unsupported file type: ' + fileType);
    }

    // ── File removal ──────────────────────────────────────────────────

    removeFile(fileId) {
        this.files.delete(fileId);
        this.removeFromOPFS(fileId);
        const metadata = this.getMetadataFromStorage().filter(m => m.id !== fileId);
        localStorage.setItem(this.storageKey, JSON.stringify(metadata));
    }

    async clearAllFiles() {
        this.files.clear();
        localStorage.removeItem(this.storageKey);
        await this.clearOPFS();
    }

    // ── Metadata helpers ──────────────────────────────────────────────

    getFileMetadata() {
        return this.getMetadataFromStorage();
    }

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

    generateFileId(fileName) {
        return `file_${Date.now()}_${Math.random().toString(36).substr(2, 9)}_${fileName.replace(/[^a-zA-Z0-9]/g, '_')}`;
    }

    getContentTypeFromExtension(fileName) {
        const ext = fileName.toLowerCase().split('.').pop();
        return { csv: 'text/csv', json: 'application/json', parquet: 'application/octet-stream', txt: 'text/plain' }[ext] || 'application/octet-stream';
    }

    saveMetadataToStorage(metadata) {
        const existing = this.getMetadataFromStorage();
        const index = existing.findIndex(m => m.id === metadata.id);
        if (index >= 0) existing[index] = metadata;
        else existing.push(metadata);
        localStorage.setItem(this.storageKey, JSON.stringify(existing));
    }

    getMetadataFromStorage() {
        try {
            const stored = localStorage.getItem(this.storageKey);
            return stored ? JSON.parse(stored) : [];
        } catch {
            return [];
        }
    }

    notify(eventName, data) {
        this.dispatchEvent(new CustomEvent(eventName, { detail: data }));
    }
}

// Create global instance
window.fileManager = new FileManager();

// Global API
globalThis.FileManagerInterop = {
    initialize: () => window.fileManager.initialize(),
    setupFileHandling: (fileInputId, dropZoneId) => window.fileManager.setupFileHandling(fileInputId, dropZoneId),
    loadFileIntoDatabase: (fileId) => window.fileManager.loadFileIntoDatabase(fileId),
    removeFile: (fileId) => window.fileManager.removeFile(fileId),
    clearAllFiles: () => window.fileManager.clearAllFiles(),
    getFileMetadata: () => window.fileManager.getFileMetadata(),
    loadFileIntoDatabasePglite: (fileId) => window.fileManager.loadFileIntoDatabasePglite(fileId)
};
