# MCP Configuration Examples

## Claude Desktop

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

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

## Windsurf

Add to your Windsurf MCP configuration:

```json
{
  "mcp": {
    "servers": {
      "biznisweb": {
        "command": "python",
        "args": ["-m", "biznisweb_mcp"],
        "cwd": "/path/to/biznisweb-mcp-server",
        "env": {
          "BIZNISWEB_API_TOKEN": "your_token_here",
          "BIZNISWEB_API_URL": "https://www.vevo.sk/api/graphql"
        }
      }
    }
  }
}
```

## Using with uv (Recommended)

If you have `uv` installed, you can run the server more efficiently:

```json
{
  "mcpServers": {
    "biznisweb": {
      "command": "uv",
      "args": ["run", "biznisweb-mcp"],
      "cwd": "/path/to/biznisweb-mcp-server",
      "env": {
        "BIZNISWEB_API_TOKEN": "your_token_here"
      }
    }
  }
}
```

## Direct Python Path

If you prefer to use the full Python path:

```json
{
  "mcpServers": {
    "biznisweb": {
      "command": "/usr/bin/python3",
      "args": ["/path/to/biznisweb-mcp-server/biznisweb_mcp/server.py"],
      "env": {
        "BIZNISWEB_API_TOKEN": "your_token_here"
      }
    }
  }
}
```

## Environment Variables from File

You can also load environment variables from a `.env` file:

```json
{
  "mcpServers": {
    "biznisweb": {
      "command": "python",
      "args": ["-m", "biznisweb_mcp"],
      "cwd": "/path/to/biznisweb-mcp-server"
    }
  }
}
```

Make sure you have a `.env` file in the server directory with:
```
BIZNISWEB_API_TOKEN=your_token_here
BIZNISWEB_API_URL=https://vevo.flox.sk/api/graphql
```