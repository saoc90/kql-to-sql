// Native Monaco Editor Implementation using AMD/require.js (simplified)
console.log('🚀 Monaco Editor AMD script starting...');

// Configure Monaco Environment for proper worker loading
window.MonacoEnvironment = {
    getWorker: function (moduleId, label) {
        console.log('🔧 Getting worker for:', label, 'moduleId:', moduleId);
        if (label === 'kusto') {
            return new Worker('./vs/base/worker/workerMain.js', {
                name: 'kusto-worker'
            });
        }
        return new Worker('./vs/base/worker/workerMain.js');
    }
};

// Create the native Monaco editor object
window.nativeMonacoEditor = {
    editors: new Map(),
    initialized: false,
    monaco: null,
    kustoLoaded: false,
    _schemaVersion: 1,

    // Bump majorVersion on all databases in a normalized schema so the
    // Kusto worker's convertToKustoJsSchemaV2 cache is invalidated and
    // fresh DatabaseSymbol objects are created with the latest tables.
    _bumpSchemaVersion(normalized) {
        this._schemaVersion++;
        if (normalized?.cluster?.databases) {
            for (const db of normalized.cluster.databases) {
                db.majorVersion = this._schemaVersion;
            }
        }
        return normalized;
    },

    // Initialize Monaco Editor environment
    async initialize() {
        console.log('🎯 Initializing Monaco Editor with AMD...');
        
        if (this.initialized) {
            console.log('✅ Monaco already initialized');
            return this.monaco;
        }

        return new Promise((resolve, reject) => {
            // Load Monaco Editor and Kusto language support
            require(['vs/editor/editor.main', 'vs/language/kusto/monaco.contribution'], () => {
                console.log('✅ Monaco Editor and Kusto language loaded');
                this.monaco = window.monaco;
                this.initialized = true;
                this.kustoLoaded = true;
                resolve(this.monaco);
            }, (error) => {
                console.error('❌ Failed to load Monaco Editor:', error);
                reject(error);
            });
        });
    },

    // Create an editor instance
    async createEditor(containerId, options = {}) {
        console.log('🎯 Creating Monaco editor...');

        if (!this.initialized) {
            await this.initialize();
        }

        const container = document.getElementById(containerId);
        if (!container) {
            throw new Error(`Container with id '${containerId}' not found`);
        }

        // Default options
        const defaultOptions = {
            value: 'StormEvents | take 10',
            language: 'kusto',
            selectionHighlight: false,
            theme: 'kusto-light',
            folding: true,
            suggest: {
                selectionMode: 'whenQuickSuggestion',
                selectQuickSuggestions: false,
            },
            'semanticHighlighting.enabled': true,
            automaticLayout: true,
            minimap: { enabled: false },
            scrollBeyondLastLine: false,
            wordWrap: 'on',
            lineNumbers: 'on',
            renderLineHighlight: 'line'
        };

        const editorOptions = { ...defaultOptions, ...options };
        
        console.log('🔧 Creating editor with options:', editorOptions);
        const editor = this.monaco.editor.create(container, editorOptions);
        
        // Store the editor
        this.editors.set(containerId, editor);
        
        console.log('✅ Monaco editor created successfully');
        return editor;
    },

    // Create an editor instance with schema
    async createEditorWithSchema(containerId, schema, options = {}) {
        console.log('🎯 Creating Monaco editor with schema...');
        
        // Create the editor first
        const editor = await this.createEditor(containerId, options);
        
        // Apply the schema
        if (schema) {
            await this.setSchemaForEditor(editor, schema);
        }
        
        return editor;
    },

    // Set schema for a specific editor
    async setSchemaForEditor(editor, schema, clusterUri = 'https://help.kusto.windows.net', databaseName = 'Samples') {
        console.log('🔧 Setting Kusto schema for specific editor...');

        if (!this.kustoLoaded) {
            await this.initialize();
        }

        try {
            const workerAccessor = await this.monaco.languages.kusto.getKustoWorker();
            const model = editor.getModel();
            const worker = await workerAccessor(model.uri);
            const normalized = await worker.normalizeSchema(schema, clusterUri, databaseName);
            this._bumpSchemaVersion(normalized);
            await worker.setSchema(normalized);
            console.log('✅ Schema applied to editor');
        } catch (error) {
            console.error('❌ Failed to set schema:', error);
            throw error;
        }
    },

    // Create StormEvents demo editor
    async createStormEventsDemo(containerId = 'root') {
        const schema = this.getDefaultStormEventsSchema();
        return await this.createEditorWithSchema(containerId, schema);
    },

    // Update schema dynamically for all editors
    async updateSchema(schema, clusterUri = 'https://help.kusto.windows.net', databaseName = 'Samples') {
        console.log('🔧 Updating Kusto schema for all editors...');

        if (!this.kustoLoaded) {
            await this.initialize();
        }

        try {
            const workerAccessor = await this.monaco.languages.kusto.getKustoWorker();

            // Apply schema to all editors using normalizeSchema + setSchema
            for (const [containerId, editor] of this.editors) {
                const model = editor.getModel();
                const worker = await workerAccessor(model.uri);
                const normalized = await worker.normalizeSchema(schema, clusterUri, databaseName);
                this._bumpSchemaVersion(normalized);
                await worker.setSchema(normalized);
                console.log(`✅ Schema updated for editor: ${containerId}`);
            }

        } catch (error) {
            console.error('❌ Failed to update schema:', error);
            throw error;
        }
    },

    // Get unified schema from ALL backends (DuckDB + PGlite) for IntelliSense
    async getUnifiedSchema() {
        const kustoTables = {};

        // --- DuckDB tables ---
        try {
            if (typeof window.DuckDbInterop !== 'undefined' && window.DuckDbInterop.getDatabaseSchema) {
                const dbSchema = await window.DuckDbInterop.getDatabaseSchema();
                if (dbSchema?.database?.tables?.length > 0) {
                    for (const table of dbSchema.database.tables) {
                        kustoTables[table.name] = {
                            Name: table.name,
                            DocString: `Table: ${table.name} (DuckDB)`,
                            OrderedColumns: table.columns.map(col => ({
                                Name: col.name,
                                Type: this.duckDbTypeToSystemType(this.kustoTypeToDuckDbType(col.type)),
                                CslType: col.type,
                                DocString: `Column: ${col.name} (${col.type})`
                            }))
                        };
                    }
                    console.log(`📊 Found ${dbSchema.database.tables.length} DuckDB table(s)`);
                }
            }
        } catch (e) {
            console.warn('⚠️ Failed to get DuckDB schema:', e);
        }

        // --- PGlite tables ---
        try {
            if (typeof window.PGliteInterop !== 'undefined' && window.PGliteInterop.getDatabaseSchema) {
                const pgSchema = await window.PGliteInterop.getDatabaseSchema();
                if (pgSchema?.database?.tables?.length > 0) {
                    for (const table of pgSchema.database.tables) {
                        // Only add if not already present from DuckDB (avoid duplicates)
                        if (!kustoTables[table.name]) {
                            kustoTables[table.name] = {
                                Name: table.name,
                                DocString: `Table: ${table.name} (PGlite)`,
                                OrderedColumns: table.columns.map(col => ({
                                    Name: col.name,
                                    Type: this.duckDbTypeToSystemType(this.kustoTypeToDuckDbType(col.type)),
                                    CslType: col.type,
                                    DocString: `Column: ${col.name} (${col.type})`
                                }))
                            };
                        }
                    }
                    console.log(`📊 Found ${pgSchema.database.tables.length} PGlite table(s)`);
                }
            }
        } catch (e) {
            console.warn('⚠️ Failed to get PGlite schema:', e);
        }

        if (Object.keys(kustoTables).length === 0) {
            console.log('ℹ️ No tables found in any backend, using default schema');
            return this.getDefaultStormEventsSchema();
        }

        return {
            Plugins: [],
            Databases: {
                Samples: {
                    Name: 'Samples',
                    Tables: kustoTables,
                    Functions: {}
                }
            }
        };
    },

    // Get current DuckDB schema and convert to Kusto format
    async getDuckDbSchema() {
        try {
            console.log('🔍 Getting DuckDB schema directly from DuckDB...');
            
            // Try to get live schema from DuckDB using the global getDatabaseSchema function
            if (typeof window.DuckDbInterop !== 'undefined' && window.DuckDbInterop.getDatabaseSchema) {
                console.log('📊 Fetching live schema from DuckDB...');
                const dbSchema = await window.DuckDbInterop.getDatabaseSchema();
                
                if (dbSchema && dbSchema.database && dbSchema.database.tables && dbSchema.database.tables.length > 0) {
                    console.log(`✅ Found ${dbSchema.database.tables.length} tables in live DuckDB schema`);

                    // Build Kusto schema directly from the already-mapped Kusto types
                    // (getDatabaseSchema already converts DuckDB types to Kusto types)
                    const kustoTables = {};
                    dbSchema.database.tables.forEach(table => {
                        kustoTables[table.name] = {
                            Name: table.name,
                            DocString: `Table: ${table.name}`,
                            OrderedColumns: table.columns.map(col => ({
                                Name: col.name,
                                Type: this.duckDbTypeToSystemType(this.kustoTypeToDuckDbType(col.type)),
                                CslType: col.type, // Already a Kusto type from getDatabaseSchema
                                DocString: `Column: ${col.name} (${col.type})`
                            }))
                        };
                    });

                    return {
                        Plugins: [],
                        Databases: {
                            Samples: {
                                Name: 'Samples',
                                Tables: kustoTables,
                                Functions: {}
                            }
                        }
                    };
                }
            }
            
            // If we have cached schema from Blazor, use it
            if (window.duckDbSchemaCache) {
                console.log('✅ Using cached DuckDB schema');
                return this.convertDuckDbToKustoSchema(window.duckDbSchemaCache);
            }
            
            // Otherwise fall back to default
            console.log('ℹ️ No live or cached schema available, using default');
            return this.getDefaultStormEventsSchema();
        } catch (error) {
            console.error('❌ Failed to get DuckDB schema:', error);
            return this.getDefaultStormEventsSchema();
        }
    },

    // Set DuckDB tables cache (called from Blazor)
    setDuckDbTablesCache(tables) {
        console.log(`🔧 Caching ${tables.length} DuckDB tables`);
        window.duckDbSchemaCache = tables;
    },

    // Get default table structure if DuckDB is empty
    getDefaultTableStructure() {
        return [
            {
                name: 'StormEvents',
                columns: [
                    { name: 'StartTime', type: 'TIMESTAMP', description: 'The start time' },
                    { name: 'EndTime', type: 'TIMESTAMP', description: 'The end time' },
                    { name: 'EpisodeId', type: 'INTEGER', description: 'Episode identifier' },
                    { name: 'EventId', type: 'INTEGER', description: 'Event identifier' },
                    { name: 'State', type: 'VARCHAR', description: 'State name' },
                    { name: 'EventType', type: 'VARCHAR', description: 'Type of storm event' },
                    { name: 'DamageProperty', type: 'DOUBLE', description: 'Property damage amount' },
                    { name: 'Source', type: 'VARCHAR', description: 'Data source' }
                ]
            }
        ];
    },

    // Convert DuckDB table schema to Kusto schema format
    convertDuckDbToKustoSchema(tables) {
        const kustoTables = {};
        
        tables.forEach(table => {
            kustoTables[table.name] = {
                Name: table.name,
                DocString: `Table: ${table.name}`,
                OrderedColumns: table.columns.map(col => ({
                    Name: col.name,
                    Type: this.duckDbTypeToSystemType(col.type),
                    CslType: this.duckDbTypeToCslType(col.type),
                    DocString: col.description || `Column: ${col.name}`
                }))
            };
        });

        return {
            Plugins: [],
            Databases: {
                Samples: {
                    Name: 'Samples',
                    Tables: kustoTables,
                    Functions: {}
                }
            }
        };
    },

    // Convert DuckDB types to .NET System types
    duckDbTypeToSystemType(duckDbType) {
        const typeMap = {
            'INTEGER': 'System.Int32',
            'BIGINT': 'System.Int64',
            'VARCHAR': 'System.String',
            'DOUBLE': 'System.Double',
            'BOOLEAN': 'System.Boolean',
            'TIMESTAMP': 'System.DateTime',
            'DATE': 'System.DateTime',
            'TIME': 'System.TimeSpan'
        };
        return typeMap[duckDbType.toUpperCase()] || 'System.String';
    },

    // Convert DuckDB types to Kusto CSL types
    duckDbTypeToCslType(duckDbType) {
        const typeMap = {
            'INTEGER': 'int',
            'BIGINT': 'long',
            'VARCHAR': 'string',
            'DOUBLE': 'real',
            'BOOLEAN': 'bool',
            'TIMESTAMP': 'datetime',
            'DATE': 'datetime',
            'TIME': 'timespan'
        };
        return typeMap[duckDbType.toUpperCase()] || 'string';
    },

    // Convert Kusto types back to DuckDB types (for schema consistency)
    kustoTypeToDuckDbType(kustoType) {
        const typeMap = {
            'int': 'INTEGER',
            'long': 'BIGINT', 
            'string': 'VARCHAR',
            'real': 'DOUBLE',
            'bool': 'BOOLEAN',
            'datetime': 'TIMESTAMP',
            'timespan': 'TIME'
        };
        return typeMap[kustoType.toLowerCase()] || 'VARCHAR';
    },

    // Get default StormEvents schema
    getDefaultStormEventsSchema() {
        return {
            Plugins: [],
            Databases: {
                Samples: {
                    Name: 'Samples',
                    Tables: {
                        StormEvents: {
                            Name: 'StormEvents',
                            DocString:
                                'A dummy description to test that docstring shows as expected when hovering over a table',
                            OrderedColumns: [
                                {
                                    Name: 'StartTime',
                                    Type: 'System.DateTime',
                                    CslType: 'datetime',
                                    DocString: 'The start time',
                                },
                                {
                                    Name: 'EndTime',
                                    Type: 'System.DateTime',
                                    CslType: 'datetime',
                                    DocString: 'The end time',
                                },
                                {
                                    Name: 'EpisodeId',
                                    Type: 'System.Int32',
                                    CslType: 'int',
                                },
                                {
                                    Name: 'EventId',
                                    Type: 'System.Int32',
                                    CslType: 'int',
                                },
                                {
                                    Name: 'State',
                                    Type: 'System.String',
                                    CslType: 'string',
                                },
                            ],
                        },
                    },
                    Functions: {},
                },
            },
        };
    },

    // Create editor with database schema (DuckDB + PGlite)
    async createEditorWithDuckDbSchema(containerId, options = {}) {
        console.log('🎯 Creating Monaco editor with database schema...');

        // Create the editor first
        const editor = await this.createEditor(containerId, options);

        // Get and apply unified schema from all backends
        const schema = await this.getUnifiedSchema();
        await this.setSchemaForEditor(editor, schema, 'https://help.kusto.windows.net', 'Samples');

        return editor;
    },

    // Set schema for Kusto language support
    async setSchema(schema, clusterUri = 'https://help.kusto.windows.net', databaseName = 'Samples') {
        console.log('🔧 Setting Kusto schema...');

        if (!this.kustoLoaded) {
            await this.initialize();
        }

        try {
            const workerAccessor = await this.monaco.languages.kusto.getKustoWorker();

            // Apply schema to all editors using normalizeSchema + setSchema
            for (const [containerId, editor] of this.editors) {
                const model = editor.getModel();
                const worker = await workerAccessor(model.uri);
                const normalized = await worker.normalizeSchema(schema, clusterUri, databaseName);
                this._bumpSchemaVersion(normalized);
                await worker.setSchema(normalized);
                console.log(`✅ Schema applied to editor: ${containerId}`);
            }

        } catch (error) {
            console.error('❌ Failed to set schema:', error);
            throw error;
        }
    },

    // Get editor instance
    getEditor(containerId) {
        return this.editors.get(containerId);
    },

    // Get Monaco instance
    getMonaco() {
        return this.monaco;
    },

    // Get editor value
    getValue(containerId) {
        const editor = this.getEditor(containerId);
        return editor ? editor.getValue() : '';
    },

    // Set editor value
    setValue(containerId, value) {
        const editor = this.getEditor(containerId);
        if (editor) {
            editor.setValue(value);
        }
    },

    // Set editor language
    setLanguage(containerId, language) {
        const editor = this.getEditor(containerId);
        if (editor) {
            const model = editor.getModel();
            if (model) {
                this.monaco.editor.setModelLanguage(model, language);
                console.log(`✅ Language changed to: ${language} for editor: ${containerId}`);
            }
        }
    },

    // Dispose editor
    dispose(containerId) {
        const editor = this.editors.get(containerId);
        if (editor) {
            editor.dispose();
            this.editors.delete(containerId);
            console.log(`🗑️ Editor disposed: ${containerId}`);
        }
    },

    // Dispose all editors
    disposeAll() {
        for (const [containerId] of this.editors) {
            this.dispose(containerId);
        }
    },

    // Add keyboard shortcut to editor
    addKeyboardShortcut(containerId, keyCombo, dotnetRef, methodName) {
        const editor = this.getEditor(containerId);
        if (editor && this.monaco) {
            // Parse key combination (e.g., "Shift+Enter")
            let keyCode;
            let ctrlKey = false;
            let shiftKey = false;
            let altKey = false;
            let metaKey = false;

            const parts = keyCombo.split('+');
            const key = parts[parts.length - 1].toLowerCase();
            
            // Check for modifier keys
            for (let i = 0; i < parts.length - 1; i++) {
                const modifier = parts[i].toLowerCase();
                switch (modifier) {
                    case 'ctrl':
                    case 'control':
                        ctrlKey = true;
                        break;
                    case 'shift':
                        shiftKey = true;
                        break;
                    case 'alt':
                        altKey = true;
                        break;
                    case 'meta':
                    case 'cmd':
                        metaKey = true;
                        break;
                }
            }

            // Map key names to Monaco key codes
            switch (key) {
                case 'enter':
                    keyCode = this.monaco.KeyCode.Enter;
                    break;
                case 'space':
                    keyCode = this.monaco.KeyCode.Space;
                    break;
                case 'escape':
                    keyCode = this.monaco.KeyCode.Escape;
                    break;
                case 'f5':
                    keyCode = this.monaco.KeyCode.F5;
                    break;
                default:
                    // For single character keys, use the char code
                    if (key.length === 1) {
                        keyCode = key.toUpperCase().charCodeAt(0);
                    } else {
                        console.warn(`Unsupported key: ${key}`);
                        return;
                    }
            }

            // Add the keyboard shortcut
            editor.addCommand(keyCode | (ctrlKey ? this.monaco.KeyMod.CtrlCmd : 0) | 
                                      (shiftKey ? this.monaco.KeyMod.Shift : 0) | 
                                      (altKey ? this.monaco.KeyMod.Alt : 0), 
                () => {
                    console.log(`🎹 Keyboard shortcut triggered: ${keyCombo}`);
                    try {
                        dotnetRef.invokeMethodAsync(methodName);
                    } catch (error) {
                        console.error(`❌ Failed to invoke .NET method ${methodName}:`, error);
                    }
                });

            console.log(`✅ Keyboard shortcut added: ${keyCombo} -> ${methodName}`);
        } else {
            console.warn(`❌ Cannot add keyboard shortcut: editor not found for ${containerId}`);
        }
    }
};

console.log('✅ Native Monaco Editor object created');

// Global functions for Blazor interop
window.updateMonacoSchema = async function(schema) {
    try {
        await window.nativeMonacoEditor.updateSchema(schema);
        console.log('✅ Schema updated from Blazor');
    } catch (error) {
        console.error('❌ Failed to update schema from Blazor:', error);
    }
};

window.setDuckDbTables = async function(tables) {
    try {
        // Cache the tables first
        window.nativeMonacoEditor.setDuckDbTablesCache(tables);
        
        // Convert to Kusto schema and update all editors
        const schema = window.nativeMonacoEditor.convertDuckDbToKustoSchema(tables);
        await window.nativeMonacoEditor.updateSchema(schema, 'https://help.kusto.windows.net', 'Samples');
        console.log('✅ DuckDB schema updated from Blazor');
    } catch (error) {
        console.error('❌ Failed to set DuckDB tables from Blazor:', error);
    }
};

window.refreshEditorSchema = async function() {
    try {
        // Invalidate cache so getDuckDbSchema fetches live data
        window.duckDbSchemaCache = null;

        // Gather tables from ALL backends so IntelliSense covers every loaded table
        const schema = await window.nativeMonacoEditor.getUnifiedSchema();

        window._latestKustoSchema = schema;
        if (window.nativeMonacoEditor.editors.size === 0) {
            console.log('ℹ️ No active editors – schema cached for next editor creation');
            return;
        }
        // Use normalizeSchema + setSchema for reliable schema application
        const workerAccessor = await window.nativeMonacoEditor.monaco.languages.kusto.getKustoWorker();
        for (const [containerId, editor] of window.nativeMonacoEditor.editors) {
            const model = editor.getModel();
            if (model) {
                try {
                    const worker = await workerAccessor(model.uri);
                    const normalized = await worker.normalizeSchema(schema, 'https://help.kusto.windows.net', 'Samples');
                    window.nativeMonacoEditor._bumpSchemaVersion(normalized);
                    await worker.setSchema(normalized);
                    console.log(`✅ Schema applied to editor: ${containerId} (${Object.keys(schema.Databases?.Samples?.Tables || {}).length} tables, v${window.nativeMonacoEditor._schemaVersion})`);
                } catch (e) {
                    console.warn(`⚠️ Failed to apply schema to ${containerId}:`, e);
                }
            }
        }

        console.log('✅ Editor schema refreshed');
    } catch (error) {
        console.error('❌ Failed to refresh editor schema:', error);
    }
};

window.ensureMonacoEditor = async function(containerId) {
    try {
        console.log(`🎯 ensureMonacoEditor called for container: ${containerId}`);
        
        // Check if editor already exists
        const existingEditor = window.nativeMonacoEditor.getEditor(containerId);
        if (existingEditor) {
            console.log('✅ Monaco Editor already exists, refreshing schema...');
            // Refresh schema even if editor exists
            try {
                const schema = await window.nativeMonacoEditor.getUnifiedSchema();
                await window.nativeMonacoEditor.setSchemaForEditor(existingEditor, schema, 'https://help.kusto.windows.net', 'Samples');
                console.log('✅ Schema refreshed for existing editor');
            } catch (schemaError) {
                console.warn('⚠️ Failed to refresh schema:', schemaError);
            }
            return { success: true, message: 'Editor already exists, schema refreshed' };
        }
        
        console.log('🔍 No existing editor found, creating new one...');
        
        // Wait for container and create editor
        console.log('⏳ Waiting for container...');
        await window.waitForContainer(containerId);
        console.log('✅ Container found, creating editor...');
        
        // Create editor with live DuckDB schema instead of default
        const editor = await window.nativeMonacoEditor.createEditorWithDuckDbSchema(containerId);
        console.log('✅ Monaco Editor created with DuckDB schema');
        
        // Store reference globally for debugging (but don't return it)
        window.kustoEditor = editor;
        
        return { success: true, message: 'Editor created with DuckDB schema' };
    } catch (error) {
        console.error('❌ Failed to ensure Monaco Editor:', error);
        console.error('❌ Error details:', error.message, error.stack);
        return { success: false, message: error.message };
    }
};

window.setEditorLanguage = function(containerId, language) {
    try {
        window.nativeMonacoEditor.setLanguage(containerId, language);
        console.log(`✅ Language set to ${language} for container ${containerId}`);
        return { success: true, message: `Language changed to ${language}` };
    } catch (error) {
        console.error('❌ Failed to set editor language:', error);
        return { success: false, message: error.message };
    }
};
