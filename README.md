# SDMX MCP Gateway

A Model Context Protocol (MCP) server that provides progressive discovery tools for SDMX statistical data. This implementation enables AI agents to explore and access SDMX-compliant statistical data repositories through a series of interactive tools, resources, and prompts.

## 🚀 Key Innovation: Progressive Discovery

The SDMX MCP Gateway solves a critical challenge in LLM-SDMX integration: **massive metadata responses that overwhelm context windows**. Traditional SDMX queries with `references=all` can return 100KB+ of XML data. Our progressive discovery approach reduces this by **98%** to just 2-3KB total.

### The Problem
- SDMX metadata with full references can exceed 100KB
- LLMs have limited context windows
- Most metadata is unnecessary for specific queries
- Complex XML structures are difficult for LLMs to parse efficiently

### The Solution: Progressive Discovery

Instead of loading everything at once, the gateway provides a layered exploration approach:

1. **Overview** (~300 bytes) - Find relevant dataflows by keyword
2. **Structure** (~1KB) - Understand dimensions and their order
3. **Drill-down** (~500 bytes) - Get codes for specific dimensions only
4. **Availability** (~700 bytes) - Check what data actually exists
5. **Query** (~200 bytes) - Build the final data URL

**Total: ~2.5KB vs 100KB+ for traditional approaches**

## Architecture Overview

The server follows a clean modular architecture with both standard and progressive discovery capabilities:

```
sdmx-mcp-gateway/
├── main_server.py                # FastMCP server entry point
├── utils.py                      # Shared utilities and constants  
├── sdmx_client.py                # Standard SDMX 2.1 REST API client
├── sdmx_progressive_client.py    # Enhanced client with progressive discovery
├── tools/
│   ├── sdmx_tools.py             # Standard discovery tools
│   └── sdmx_progressive_tools.py # Progressive discovery tools
├── resources/
│   └── sdmx_resources.py         # Metadata browsing resources
└── prompts/
    └── sdmx_prompts.py           # Guided query construction prompts
```

## Current Implementation Status

### FULLY IMPLEMENTED ✓

#### 1. Progressive Discovery Tools (NEW!)
- **`discover_dataflows_overview()`**: Lightweight dataflow discovery (~300 bytes per dataflow)
- **`get_dataflow_structure()`**: Get dimension order and structure without full codelists (~1KB)
- **`explore_dimension_codes()`**: Drill down into specific dimensions with search (~500 bytes)
- **`check_data_availability()`**: Query actual data availability from ContentConstraints (~700 bytes)
- **`build_data_query()`**: Construct validated data URLs with dimension dictionaries or keys
- **`get_discovery_guide()`**: Interactive guide for the discovery workflow

#### 2. Standard Discovery Tools
- **`discover_dataflows()`**: Find available statistical domains with keyword filtering
- **`get_structure()`**: Explore dataflow dimensions, attributes, and organization
- **`browse_codelist()`**: Browse specific dimension codes (countries, indicators, etc.)
- **`validate_syntax()`**: Validate query parameters against SDMX 2.1 specification
- **`build_query()`**: Generate final data URLs in multiple formats (CSV, JSON, XML)

#### 3. SDMX 2.1 REST API Client
- **Standards Compliance**: Full implementation based on SDMX 2.1 OpenAPI specification
- **Multiple Endpoints**: Support for dataflow, datastructure, and codelist endpoints
- **DSD Discovery**: Automatic extraction of Data Structure Definition IDs from dataflow metadata
- **Error Handling**: Robust error handling with meaningful error messages
- **Session Management**: Efficient HTTP session management with connection pooling
- **XML Parsing**: Complete SDMX-ML parsing with proper namespace handling
- **ContentConstraint Support**: Parse actual data availability from `type="Actual"` constraints

#### 4. MCP Resources for Metadata Browsing
- **Agency Directory**: `sdmx://agencies` - List of known SDMX data providers
- **Agency Information**: `sdmx://agency/{id}/info` - Specific agency details
- **Format Guide**: `sdmx://formats/guide` - Data format comparison and use cases
- **Syntax Guide**: `sdmx://syntax/guide` - SDMX query syntax reference

#### 5. Guided Prompts System
- **Discovery Guide**: Step-by-step data discovery workflow
- **Troubleshooting Guide**: Common issue resolution strategies
- **Best Practices**: Use-case specific guidance (research, dashboards, automation)
- **Query Builder**: Interactive query construction assistance

#### 6. Development Infrastructure
- **Modular Design**: Clean separation of concerns across multiple files
- **Type Hints**: Full type annotation for better code quality
- **Logging**: Comprehensive logging throughout the application
- **Dependencies**: Managed via pyproject.toml with locked versions

## Key Technical Improvements

### 1. Automatic DSD Discovery
The gateway correctly handles the common SDMX pattern where Data Structure Definition (DSD) IDs differ from dataflow IDs:
- Extracts DSD reference from dataflow's `<structure:Structure>` element
- Handles both namespaced and non-namespaced `<Ref>` elements
- Example: `DF_DIGITAL_DEVELOPMENT` → `DSD_DIGITAL_DEVELOPMENT`

### 2. Dimension Ordering
Properly maintains dimension order which is critical for SDMX key construction:
- Parses dimension positions from the DSD
- Generates correct key templates (e.g., `{FREQ}.{TIME_PERIOD}.{GEO_PICT}.{INDICATOR}`)
- Supports both positional and dictionary-based key construction

### 3. ContentConstraint Parsing
Implements actual data availability checking via ContentConstraint with `type="Actual"`:
- Shows what data actually exists vs theoretical structure
- Extracts time ranges and valid dimension combinations
- Reduces failed queries by validating against actual availability

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

## Usage Examples

### Progressive Discovery Workflow (Recommended for LLMs)

Here's how to find and query Tonga's digital development indicators for 2020:

```python
# Step 1: Find relevant dataflows (lightweight overview)
discover_dataflows_overview(keywords=["digital", "development"])
# → Returns: DF_DIGITAL_DEVELOPMENT (~300 bytes)

# Step 2: Get structure without loading all codelists
get_dataflow_structure("DF_DIGITAL_DEVELOPMENT")
# → Returns: Dimensions order: FREQ.TIME_PERIOD.GEO_PICT.INDICATOR (~1KB)

# Step 3: Find Tonga's code
explore_dimension_codes("DF_DIGITAL_DEVELOPMENT", "GEO_PICT", search="tonga")
# → Returns: TO = Tonga (~200 bytes)

# Step 4: Check what indicators are available
explore_dimension_codes("DF_DIGITAL_DEVELOPMENT", "INDICATOR", limit=5)
# → Returns: First 5 indicator codes (~500 bytes)

# Step 5: Build the query
build_data_query(
    "DF_DIGITAL_DEVELOPMENT",
    dimensions={"FREQ": "A", "TIME_PERIOD": "2020", "GEO_PICT": "TO"},
    format="csv"
)
# → Returns: Ready-to-use URL (~200 bytes)
```

**Total data transferred: ~2.2KB** (vs 100KB+ with traditional approach)

### Standard Discovery Workflow

For cases where you need complete metadata:

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

#### Option 1: Using UV (Recommended)
```bash
# Navigate to project directory
cd sdmx-mcp-gateway

# Install UV if not already installed
# See: https://docs.astral.sh/uv/getting-started/installation/

# Sync dependencies (creates virtual environment automatically)
uv sync

# Test the server
uv run python main_server.py

# Or use MCP development tools
uv run mcp dev ./main_server.py
```

#### Option 2: Using pip
```bash
# Navigate to project directory
cd sdmx-mcp-gateway

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

##### Windows Configuration

Add to `%APPDATA%\Claude\claude_desktop_config.json`:

**Using UV (Recommended):**
```json
{
  "mcpServers": {
    "sdmx-gateway": {
      "command": "uv",
      "args": ["run", "--directory", "C:\\Users\\YOUR_USERNAME\\path\\to\\sdmx-mcp-gateway", "python", "main_server.py"]
    }
  }
}
```

**Using Python directly:**
```json
{
  "mcpServers": {
    "sdmx-gateway": {
      "command": "python",
      "args": ["C:\\Users\\YOUR_USERNAME\\path\\to\\sdmx-mcp-gateway\\main_server.py"]
    }
  }
}
```

**Important Windows Notes:**
- Use double backslashes (`\\`) in paths
- Use full absolute paths (the `cwd` parameter doesn't work reliably on Windows)
- If using UV, first run `uv sync` in the project directory to install dependencies
- Restart Claude Desktop after changing the configuration

##### macOS/Linux Configuration

Add to `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or `~/.config/Claude/claude_desktop_config.json` (Linux):

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

## Known Issues & Future Enhancements

### Multi-User Considerations
⚠️ **Warning**: The current endpoint switching implementation uses global state, which means in a multi-user server deployment, when one user switches endpoints (e.g., from SPC to ECB), it affects ALL users. This is suitable for:
- Single-user Claude Desktop integration
- Local development and testing
- Single-tenant deployments

For multi-user production deployments, see `MULTI_USER_CONSIDERATIONS.md`.

### Planned Enhancements

1. **Enhanced Data Availability Checking**: 
   - Current limitation: The availability tool checks if dimensions exist individually, but not their combinations
   - Example: There might be data for "Vanuatu" and data for "2024", but no data for "Vanuatu in 2024"
   - TODO: Extend the `get_data_availability` tool to check specific dimension combinations

2. **Session-Based Endpoint Configuration**:
   - Implement per-session endpoint configuration for multi-user scenarios
   - Store endpoint selection in MCP Context rather than global state

## Contributing

The project is currently in active development. Key areas for contribution:
- Enhanced semantic mapping algorithms
- Additional SDMX provider support
- Performance optimization
- Test coverage expansion
- Documentation improvements

## License

This project implements the architectural design specified in "MCP-SDMX Go Implementation" blueprint while adapting implementation details for Python ecosystem compatibility.