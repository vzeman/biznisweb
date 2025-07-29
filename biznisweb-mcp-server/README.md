# BizniWeb MCP Server

A Model Context Protocol (MCP) server that enables LLMs to interact with BizniWeb e-shop through GraphQL API.

## Features

The server provides the following tools:

### 1. `list_orders`
List orders with optional date filtering
- **Parameters:**
  - `from_date` (optional): From date in YYYY-MM-DD format
  - `to_date` (optional): To date in YYYY-MM-DD format
  - `status` (optional): Order status ID
  - `limit` (optional): Maximum number of orders to return (default: 30)

### 2. `get_order`
Get detailed information about a specific order
- **Parameters:**
  - `order_num` (required): Order number

### 3. `order_statistics`
Get order statistics for a date range
- **Parameters:**
  - `from_date` (optional): From date in YYYY-MM-DD format
  - `to_date` (optional): To date in YYYY-MM-DD format

### 4. `search_orders`
Search orders by customer name, email, or order number
- **Parameters:**
  - `query` (required): Search query

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd biznisweb-mcp-server
```

2. Install dependencies:
```bash
pip install -e .
```

3. Configure API credentials:
```bash
cp .env.example .env
# Edit .env and add your BizniWeb API token
```

## Usage

### With Claude Desktop

1. Add to your Claude Desktop configuration (`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "biznisweb": {
      "command": "python",
      "args": ["-m", "biznisweb_mcp"],
      "env": {
        "BIZNISWEB_API_TOKEN": "your_token_here",
        "BIZNISWEB_API_URL": "https://www.vevo.sk/api/graphql"
      }
    }
  }
}
```

2. Restart Claude Desktop

3. The BizniWeb tools will be available in your conversations

### With Other MCP Clients

Run the server:
```bash
python -m biznisweb_mcp
```

The server communicates via stdin/stdout using the MCP protocol.

## Example Usage in Claude

```
Use the list_orders tool to show me orders from the last 7 days

Use the get_order tool to get details for order 2502001234

Use the order_statistics tool to show me sales statistics for this month

Use the search_orders tool to find orders from customer "John Doe"
```

## Environment Variables

- `BIZNISWEB_API_TOKEN`: Your BizniWeb API token (required)
- `BIZNISWEB_API_URL`: API endpoint URL (default: https://www.vevo.sk/api/graphql)

## API Token

You can generate an API token in your BizniWeb account:
Settings → BiznisWeb API → New API Token button

## Development

To run in development mode:
```bash
python biznisweb_mcp/server.py
```

## Error Handling

The server handles:
- Missing API tokens
- Network errors
- Invalid parameters
- GraphQL query errors

All errors are returned as structured MCP error responses.

## License

This project is licensed under the MIT License.