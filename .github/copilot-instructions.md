# KQL to SQL Converter - Repository Instructions

## Project Overview

This repository contains a comprehensive KQL (Kusto Query Language) to SQL converter built on top of the official Kusto language parser. The project includes:

- **Core Library**: A .NET library (`KqlToSql`) that converts KQL queries to SQL
- **Blazor Demo**: A WebAssembly application (`DuckDbDemo`) that demonstrates the converter in action using DuckDB
- **Unit Tests**: Comprehensive test suite validating the conversion functionality

The project enables users to write KQL queries and execute them against SQL databases, bridging the gap between Azure Data Explorer (Kusto) query language and traditional SQL databases.

## Architecture and Technologies

### Core Technologies
- **.NET 9** - Target framework for all projects
- **C# 13.0** - Language version
- **Blazor WebAssembly** - For the demo application
- **DuckDB** - In-browser database for query execution
- **Monaco Editor** - Code editor with Kusto language support
- **MudBlazor** - UI component library for Blazor

### Project Structure
```
src/
├── KqlToSql/                    # Core conversion library
│   ├── Operators/               # KQL operator translators (where, project, etc.)
│   ├── Commands/                # KQL command translators (.ingest, .view, etc.)
│   └── KqlToSqlConverter.cs     # Main converter class
├── DuckDbDemo/                  # Blazor WebAssembly demo application
│   ├── Pages/                   # Razor pages (Home, FileManager)
│   ├── Services/                # Application services
│   ├── DuckDB/                  # DuckDB interop classes
│   └── wwwroot/                 # Static web assets and JavaScript
tests/
└── KqlToSql.Tests/             # Unit tests with StormEvents data
```

## Coding Standards and Conventions

### C# Standards
- Use `var` for local variable declarations when type is obvious
- Prefer expression-bodied members for simple properties and methods
- Use nullable reference types (`#nullable enable`)
- Follow async/await patterns consistently
- Use `record` types for immutable data structures where appropriate
- Implement `IDisposable` for components that manage resources

### Blazor-Specific Guidelines
- Use `@inject` directive for dependency injection
- Implement `IDisposable` for components with event handlers or JS interop
- Use `StateHasChanged()` when manually updating component state
- Prefer MudBlazor components over native HTML elements
- Use `IJSRuntime` for JavaScript interoperability with proper error handling

### JavaScript/TypeScript Standards
- Use `async/await` for asynchronous operations
- Implement proper error handling with try/catch blocks
- Use `console.log` with emojis for better debugging (🚀 for start, ✅ for success, ❌ for errors)
- Follow modern ES6+ syntax patterns
- Use proper JSDoc comments for complex functions

### Testing Standards
- Unit tests use xUnit framework
- Tests validate against real StormEvents data from NOAA
- Each operator/command should have comprehensive test coverage
- Use descriptive test method names following Given_When_Then pattern

## Development Guidelines

### Adding New KQL Operators
1. Create translator class in `src/KqlToSql/Operators/`
2. Add operator handling to `OperatorSqlTranslator.cs`
3. Write comprehensive unit tests in `tests/KqlToSql.Tests/`
4. Update KqlOperatorsChecklist.md with supported functionality

### Adding New Control Commands
1. Create command handler in `src/KqlToSql/Commands/`
2. Add command recognition to `CommandSqlTranslator.cs` 
3. Add corresponding tests with real data scenarios

### Blazor Component Development
- Use MudBlazor components for consistent UI
- Implement proper loading states and error handling
- Use dependency injection for services
- Follow async patterns for all I/O operations

### JavaScript Interop Best Practices
- Use `DotNetObjectReference` for callbacks from JS to .NET
- Implement proper disposal patterns for JS resources
- Handle browser compatibility issues gracefully
- Use proper error propagation between JS and .NET

## Common Patterns and Utilities

## Performance Considerations

- Files are processed entirely client-side to avoid server load
- Use streaming for large file processing when possible
- Implement proper memory management for JavaScript file objects
- Use efficient SQL generation to minimize DuckDB query overhead
- Cache schema information to reduce repeated computations

## Browser Compatibility

### Required Features
- WebAssembly support (all modern browsers)
- File API for file handling
- IndexedDB for client-side storage (DuckDB requirement)

### Optional Features  
- File System Access API for persistent file access (Chrome/Edge only)
- Service Workers for offline capability

## Documentation and Examples

- XML documentation comments for all public APIs
- Inline code comments for complex algorithms
