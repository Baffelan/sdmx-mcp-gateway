# SDMX MCP Gateway

A Model Context Protocol (MCP) server that provides progressive discovery tools for SDMX statistical data. This implementation enables AI agents to explore and access SDMX-compliant statistical data repositories through interactive tools, resources, and prompts.

**Version 0.2.0** - Now with structured outputs, Streamable HTTP transport, and elicitation support.

## 🚀 Key Features

- **Progressive Discovery**: Reduces metadata transfer from 100KB+ to ~2.5KB
- **Structured Outputs**: All tools return validated Pydantic models
- **Multiple Transports**: STDIO (development) and Streamable HTTP (production)
- **Interactive Elicitation**: User confirmation dialogs for endpoint switching
- **Multi-Provider Support**: SPC, ECB, UNICEF, IMF data sources

## Quick Start

```bash
# Install dependencies
cd sdmx-mcp-gateway
uv sync

# Run the server (STDIO mode for development)
uv run python main_server.py

# Run with MCP Inspector
uv run mcp dev ./main_server.py

# Run in production mode (Streamable HTTP)
uv run python main_server.py --transport http --port 8000 --stateless --json-response
```

## The Problem We Solve

Traditional SDMX queries with `references=all` return 100KB+ of XML metadata, overwhelming LLM context windows. Our progressive discovery approach provides a layered exploration:

| Step      | Operation                        | Data Size  |
| --------- | -------------------------------- | ---------- |
| 1         | Find dataflows by keyword        | ~300 bytes |
| 2         | Get dimension structure          | ~1KB       |
| 3         | Explore specific dimension codes | ~500 bytes |
| 4         | Check data availability          | ~700 bytes |
| 5         | Build final query URL            | ~200 bytes |
| **Total** |                                  | **~2.5KB** |

## Architecture

```
sdmx-mcp-gateway/
├── main_server.py              # FastMCP server with CLI
├── app_context.py              # Lifespan management & shared resources
├── config.py                   # Endpoint configuration
├── sdmx_progressive_client.py  # SDMX 2.1 REST client
├── utils.py                    # Validation & utilities
├── models/
│   ├── __init__.py
│   └── schemas.py              # Pydantic output schemas
├── tools/
│   ├── sdmx_tools.py           # Discovery tools implementation
│   └── endpoint_tools.py       # Endpoint management
├── resources/
│   └── sdmx_resources.py       # MCP resources
├── prompts/
│   └── sdmx_prompts.py         # Guided prompts
└── tests/                      # Test suite
```

## Available Tools

### Discovery Tools

| Tool                     | Description                   | Output Schema             |
| ------------------------ | ----------------------------- | ------------------------- |
| `list_dataflows`         | Find dataflows by keyword     | `DataflowListResult`      |
| `get_dataflow_structure` | Get dimensions and structure  | `DataflowStructureResult` |
| `get_dimension_codes`    | Explore codes for a dimension | `DimensionCodesResult`    |
| `get_data_availability`  | Check what data exists        | `DataAvailabilityResult`  |
| `validate_query`         | Validate query parameters     | `ValidationResult`        |
| `build_key`              | Construct SDMX key            | `KeyBuildResult`          |
| `build_data_url`         | Generate data retrieval URL   | `DataUrlResult`           |
| `get_codelist`           | Browse specific codelist      | `dict`                    |

### Endpoint Management

| Tool                          | Description                            | Output Schema          |
| ----------------------------- | -------------------------------------- | ---------------------- |
| `get_current_endpoint`        | Show active data source                | `EndpointInfo`         |
| `list_available_endpoints`    | List all providers                     | `EndpointListResult`   |
| `switch_endpoint`             | Change data provider                   | `EndpointSwitchResult` |
| `switch_endpoint_interactive` | Interactive selection with elicitation | `EndpointSwitchResult` |

### Resources

- `sdmx://agencies` - List of known SDMX data providers
- `sdmx://agency/{id}/info` - Specific agency details
- `sdmx://formats/guide` - Data format comparison
- `sdmx://syntax/guide` - Query syntax reference

### Prompts

- `discovery_guide` - Step-by-step data discovery workflow
- `troubleshooting_guide` - Common issue resolution
- `best_practices` - Use-case specific guidance
- `query_builder` - Interactive query construction

## Supported Data Sources

| Key      | Provider                    | Description                           |
| -------- | --------------------------- | ------------------------------------- |
| `SPC`    | Pacific Data Hub            | Pacific regional statistics (default) |
| `ECB`    | European Central Bank       | European financial statistics         |
| `UNICEF` | UNICEF                      | Children and youth statistics         |
| `IMF`    | International Monetary Fund | Global financial statistics           |

Switch providers using:

```python
# Direct switch
switch_endpoint("ECB")

# Interactive selection (with elicitation)
switch_endpoint_interactive()
```

## Installation

### Prerequisites

- Python 3.12 or higher
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

### Using UV (Recommended)

```bash
cd sdmx-mcp-gateway
uv sync
```

### Using pip

```bash
cd sdmx-mcp-gateway
pip install -r requirements.txt
```

### Dependencies

- `mcp[cli]>=1.26.0` - Model Context Protocol SDK
- `pydantic>=2.0.0` - Structured output validation
- `httpx>=0.27.0` - Async HTTP client
- `certifi>=2024.0.0` - SSL certificates

## Running the Server

### CLI Options

```bash
uv run python main_server.py [OPTIONS]

Options:
  --transport, -t    Transport type: stdio, http, streamable-http (default: stdio)
  --host             Host for HTTP transport (default: 127.0.0.1)
  --port, -p         Port for HTTP transport (default: 8000)
  --stateless        Run in stateless mode (HTTP only)
  --json-response    Use JSON responses instead of SSE (HTTP only)
  --debug            Enable debug logging
```

### Development Mode (STDIO)

```bash
# Direct execution
uv run python main_server.py

# With MCP Inspector (opens browser UI)
uv run mcp dev ./main_server.py
```

### Production Mode (Streamable HTTP)

```bash
uv run python main_server.py --transport http --port 8000 --stateless --json-response
```

## MCP Client Configuration

### Claude Desktop

**Linux** (`~/.config/Claude/claude_desktop_config.json`):

```json
{
    "mcpServers": {
        "sdmx-gateway": {
            "command": "uv",
            "args": [
                "run",
                "--directory",
                "/path/to/sdmx-mcp-gateway",
                "python",
                "main_server.py"
            ]
        }
    }
}
```

**macOS** (`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
    "mcpServers": {
        "sdmx-gateway": {
            "command": "uv",
            "args": [
                "run",
                "--directory",
                "/path/to/sdmx-mcp-gateway",
                "python",
                "main_server.py"
            ]
        }
    }
}
```

**Windows** (`%APPDATA%\Claude\claude_desktop_config.json`):

```json
{
    "mcpServers": {
        "sdmx-gateway": {
            "command": "uv",
            "args": [
                "run",
                "--directory",
                "C:\\path\\to\\sdmx-mcp-gateway",
                "python",
                "main_server.py"
            ]
        }
    }
}
```

### Cursor

1. Go to **Cursor Settings > MCP**
2. Add new global MCP server
3. Use the configuration above

## Usage Examples

### Progressive Discovery Workflow

```python
# Step 1: Find relevant dataflows
list_dataflows(keywords=["digital", "development"])
# → Returns: DataflowListResult with matching dataflows

# Step 2: Get structure
get_dataflow_structure("DF_DIGITAL_DEVELOPMENT")
# → Returns: DataflowStructureResult with dimensions

# Step 3: Find country code
get_dimension_codes("DF_DIGITAL_DEVELOPMENT", "GEO_PICT", search_term="tonga")
# → Returns: DimensionCodesResult with TO = Tonga

# Step 4: Check availability
get_data_availability("DF_DIGITAL_DEVELOPMENT", dimension_values={"GEO_PICT": "TO"})
# → Returns: DataAvailabilityResult with time ranges

# Step 5: Build query
build_data_url("DF_DIGITAL_DEVELOPMENT", key="A..TO.", format_type="csv")
# → Returns: DataUrlResult with ready-to-use URL
```

### Interactive Endpoint Switching

For clients that support elicitation (shows interactive form):

```python
switch_endpoint_interactive()
# → Shows form to select endpoint and confirm
# → Returns: EndpointSwitchResult
```

For clients without elicitation support:

```python
switch_endpoint("ECB")
# → Directly switches to ECB
# → Returns: EndpointSwitchResult
```

## Structured Outputs

All tools return Pydantic models with validated, typed data:

```python
# Example: DataflowListResult
{
  "discovery_level": "overview",
  "agency_id": "SPC",
  "total_found": 45,
  "showing": 10,
  "offset": 0,
  "limit": 10,
  "dataflows": [
    {"id": "DF_GDP", "name": "GDP Statistics", "description": "..."},
    ...
  ],
  "pagination": {
    "has_more": true,
    "next_offset": 10,
    "total_pages": 5,
    "current_page": 1
  },
  "next_step": "Use get_dataflow_structure() to explore a dataflow"
}
```

## Testing

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=. --cov-report=html

# Run specific test categories
uv run pytest tests/unit/
uv run pytest tests/integration/
uv run pytest tests/e2e/
```

## Known Limitations

### Multi-User Endpoint Switching

The endpoint switching uses global state. In multi-user deployments, switching affects all users. Suitable for:

- Single-user Claude Desktop integration
- Local development
- Single-tenant deployments

See `MULTI_USER_CONSIDERATIONS.md` for production alternatives.

### Elicitation Support

Interactive tools (`switch_endpoint_interactive`) require client elicitation support. Clients without support receive a helpful fallback message with available endpoints.

## Project Status

| Feature                   | Status      |
| ------------------------- | ----------- |
| SDK upgrade (v1.26.0)     | ✅ Complete |
| Structured outputs        | ✅ Complete |
| Streamable HTTP transport | ✅ Complete |
| Lifespan context          | ✅ Complete |
| Elicitation support       | ✅ Complete |
| Icons & metadata          | 🔄 Pending  |
| Documentation             | ✅ Complete |

See `TODO.md` for detailed modernization progress.

## Contributing

Key areas for contribution:

- Additional SDMX provider support
- Enhanced semantic search
- Performance optimization
- Test coverage expansion

## References

- [MCP Specification](https://modelcontextprotocol.io/specification)
- [MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk)
- [SDMX 2.1 REST API](https://github.com/sdmx-twg/sdmx-rest)
- [Pacific Data Hub](https://stats.pacificdata.org/)

## License

MIT License - See LICENSE file for details.
