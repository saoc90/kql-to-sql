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
            await worker.setSchemaFromShowSchema(schema, clusterUri, databaseName);
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
            
            // Apply schema to all editors
            for (const [containerId, editor] of this.editors) {
                const model = editor.getModel();
                const worker = await workerAccessor(model.uri);
                await worker.setSchemaFromShowSchema(schema, clusterUri, databaseName);
                console.log(`✅ Schema updated for editor: ${containerId}`);
            }
            
        } catch (error) {
            console.error('❌ Failed to update schema:', error);
            throw error;
        }
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
                    
                    // Convert DuckDB schema format to our internal format, then to Kusto
                    const tables = dbSchema.database.tables.map(table => ({
                        name: table.name,
                        columns: table.columns.map(col => ({
                            name: col.name,
                            type: this.kustoTypeToDuckDbType(col.type), // Convert back to DuckDB format for consistency
                            description: `Column: ${col.name} (${col.type})`
                        }))
                    }));
                    
                    return this.convertDuckDbToKustoSchema(tables);
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
                DuckDB: {
                    Name: 'DuckDB',
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

    // Create editor with DuckDB schema
    async createEditorWithDuckDbSchema(containerId, options = {}) {
        console.log('🎯 Creating Monaco editor with DuckDB schema...');
        
        // Create the editor first
        const editor = await this.createEditor(containerId, options);
        
        // Get and apply DuckDB schema
        const schema = await this.getDuckDbSchema();
        await this.setSchemaForEditor(editor, schema, 'https://help.kusto.windows.net', 'DuckDB');
        
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
            
            // Apply schema to all editors
            for (const [containerId, editor] of this.editors) {
                const model = editor.getModel();
                const worker = await workerAccessor(model.uri);
                await worker.setSchemaFromShowSchema(schema, clusterUri, databaseName);
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
        await window.nativeMonacoEditor.updateSchema(schema, 'https://help.kusto.windows.net', 'DuckDB');
        console.log('✅ DuckDB schema updated from Blazor');
    } catch (error) {
        console.error('❌ Failed to set DuckDB tables from Blazor:', error);
    }
};

window.refreshEditorSchema = async function() {
    try {
        const schema = await window.nativeMonacoEditor.getDuckDbSchema();
        await window.nativeMonacoEditor.updateSchema(schema);
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
                const schema = await window.nativeMonacoEditor.getDuckDbSchema();
                await window.nativeMonacoEditor.setSchemaForEditor(existingEditor, schema, 'https://help.kusto.windows.net', 'DuckDB');
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
