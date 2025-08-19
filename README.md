# SDMX MCP Gateway

A Model Context Protocol (MCP) server that provides progressive discovery tools for SDMX statistical data. This implementation enables AI agents to explore and access SDMX-compliant statistical data repositories through a series of interactive tools, resources, and prompts.

## Project Overview

The SDMX MCP Gateway provides a modular approach to SDMX data discovery. Instead of requiring users to construct complex queries upfront, it offers a step-by-step exploration process:

1. **Discover** available statistical domains (dataflows)
2. **Explore** data structure and dimensions  
3. **Browse** specific codelists and valid values
4. **Validate** query syntax before execution
5. **Build** final data retrieval URLs

This approach is much more effective for AI agents and human users who need to understand data availability and structure before constructing queries.

## Architecture Overview

The server follows a clean modular architecture based on the MCP guide recommendations:

```
sdmx-mcp-gateway/
├── main_server.py          # FastMCP server entry point
├── utils.py                # Shared utilities and constants  
├── sdmx_client.py          # SDMX 2.1 REST API client
├── tools/
│   └── sdmx_tools.py       # Progressive discovery tools
├── resources/
│   └── sdmx_resources.py   # Metadata browsing resources
└── prompts/
    └── sdmx_prompts.py     # Guided query construction prompts
```

## Current Implementation Status

### FULLY IMPLEMENTED ✓

#### 1. Progressive Discovery Tools
- **`discover_dataflows()`**: Find available statistical domains with keyword filtering
- **`get_structure()`**: Explore dataflow dimensions, attributes, and organization
- **`browse_codelist()`**: Browse specific dimension codes (countries, indicators, etc.)
- **`validate_syntax()`**: Validate query parameters against SDMX 2.1 specification
- **`build_query()`**: Generate final data URLs in multiple formats (CSV, JSON, XML)
- **Context Support**: Progress reporting and logging for long-running operations

#### 2. SDMX 2.1 REST API Client
- **Standards Compliance**: Full implementation based on SDMX 2.1 OpenAPI specification
- **Multiple Endpoints**: Support for dataflow, datastructure, and codelist endpoints
- **Error Handling**: Robust error handling with meaningful error messages
- **Session Management**: Efficient HTTP session management with connection pooling
- **XML Parsing**: Complete SDMX-ML parsing with proper namespace handling

#### 3. MCP Resources for Metadata Browsing
- **Agency Directory**: `sdmx://agencies` - List of known SDMX data providers
- **Agency Information**: `sdmx://agency/{id}/info` - Specific agency details
- **Format Guide**: `sdmx://formats/guide` - Data format comparison and use cases
- **Syntax Guide**: `sdmx://syntax/guide` - SDMX query syntax reference

#### 4. Guided Prompts System
- **Discovery Guide**: Step-by-step data discovery workflow
- **Troubleshooting Guide**: Common issue resolution strategies
- **Best Practices**: Use-case specific guidance (research, dashboards, automation)
- **Query Builder**: Interactive query construction assistance

#### 5. Development Infrastructure
- **Modular Design**: Clean separation of concerns across multiple files
- **Type Hints**: Full type annotation for better code quality
- **Logging**: Comprehensive logging throughout the application
- **Dependencies**: Managed via pyproject.toml with locked versions

### FUTURE DEVELOPMENT OPPORTUNITIES

#### 1. Advanced DSD Parsing
- **Opportunity**: Full parsing of Data Structure Definitions to extract complete dimension and attribute metadata
- **Benefit**: Would enable automatic dimension mapping (e.g., "Tonga" → "TO" in REF_AREA codelist)
- **Current Status**: Basic structure information is retrieved; full parsing would require significant SDMX-ML processing

#### 2. Caching Layer
- **Opportunity**: Implement caching for frequently accessed dataflows and codelists
- **Benefit**: Improved performance and reduced API load
- **Implementation Options**: In-memory caching for development, Redis for production

#### 3. Multi-Agency Federation
- **Opportunity**: Simultaneous search across multiple SDMX providers (ECB, OECD, EUROSTAT, etc.)
- **Benefit**: Comprehensive data discovery across the entire SDMX ecosystem
- **Current Status**: Single agency support implemented; multi-agency requires coordination logic

## Key Features

### Progressive Discovery Workflow
Instead of requiring complex upfront queries, the server enables step-by-step exploration:

```
1. discover_dataflows(keywords=["trade", "fisheries"])
   ↓
2. get_structure(dataflow_id="DF_TRADE_FOOD") 
   ↓
3. browse_codelist(codelist_id="REF_AREA", search_term="tonga")
   ↓
4. validate_syntax(dataflow_id="DF_TRADE_FOOD", key="A.TO.FISH")
   ↓
5. build_query(dataflow_id="DF_TRADE_FOOD", key="A.TO.FISH", format_type="csv")
```

### Context-Aware Operations
All long-running operations provide progress reporting and informational logging:

```python
# Example: Dataflow discovery with progress reporting
async def discover_dataflows(ctx: Context):
    ctx.info("Starting dataflow discovery...")
    await ctx.report_progress(0, 100)
    
    # Fetch and process dataflows
    ctx.info("Fetching dataflow list from SDMX API...")
    await ctx.report_progress(25, 100)
    
    # Parse results
    ctx.info(f"Processing {len(dataflows)} dataflows...")
    await ctx.report_progress(75, 100)
```

### Standards Compliance
Built on the official SDMX 2.1 REST API specification:
- Proper URL construction following SDMX patterns
- Correct MIME types for different data formats  
- Full parameter validation against SDMX regex patterns
- Support for all standard SDMX time period formats

## Testing & Validation

### Test Coverage
- **Unit Tests**: Validation functions, utility functions, and data parsing
- **Integration Tests**: MCP tool functionality with mock SDMX responses
- **End-to-End Tests**: Live API connectivity with real SDMX endpoints
- **Error Handling**: Network failures, malformed responses, and edge cases

### Running Tests
```bash
# Install test dependencies
pip install pytest pytest-asyncio pytest-mock

# Run all tests
pytest

# Run with coverage
pytest --cov=. --cov-report=html

# Run specific test categories  
pytest tests/unit/          # Unit tests only
pytest tests/integration/   # Integration tests only
pytest tests/e2e/          # End-to-end tests only
```

## Installation & Usage

### Prerequisites
- Python 3.10 or higher
- Internet connectivity for SDMX API access

### Setup
```bash
# Navigate to project directory
cd MCP/sdmx-mcp-gateway

# Install dependencies including MCP SDK
pip install mcp "mcp[cli]" httpx

# Install development dependencies (optional)
pip install pytest pytest-asyncio pytest-mock pytest-cov

# Test the server
python main_server.py

# Or use MCP development tools
mcp dev ./main_server.py
```

### MCP Integration

#### With Claude Desktop
Add to your MCP configuration file:

```json
{
  "mcpServers": {
    "sdmx-gateway": {
      "command": "python",
      "args": ["main_server.py"],
      "cwd": "/path/to/sdmx-mcp-gateway"
    }
  }
}
```

#### With Cursor
1. Go to Cursor Settings > MCP
2. Add new global MCP server
3. Use the configuration above

#### Testing with MCP CLI
```bash
# Launch development dashboard
mcp dev ./main_server.py

# This opens a browser interface where you can:
# - View all available tools and resources
# - Test tools with different parameters
# - See real-time results and logging
```

## Technical Architecture

The implementation follows a modified version of the original four-pillar architecture:

1. **MCP Interface** (Python MCP SDK)
2. **Query Interpretation Engine** (Pattern matching + scoring)
3. **SDMX Interaction Module** (HTTP client + XML parsing)
4. **Structured Output Generator** (JSON formatting, not script generation)

The server operates as a lightweight, stateless service that processes metadata only, never handling bulk statistical data transfer. This design ensures scalability and security while providing the intelligence needed for effective data discovery.

## Contributing

The project is currently in active development. Key areas for contribution:
- Enhanced semantic mapping algorithms
- Additional SDMX provider support
- Performance optimization
- Test coverage expansion
- Documentation improvements

## License

This project implements the architectural design specified in "MCP-SDMX Go Implementation" blueprint while adapting implementation details for Python ecosystem compatibility.