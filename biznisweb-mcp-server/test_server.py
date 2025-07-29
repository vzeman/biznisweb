#!/usr/bin/env python3
"""
Test script for BizniWeb MCP Server

This script tests the MCP server by simulating tool calls
"""

import asyncio
import json
import os
from datetime import datetime, timedelta
from biznisweb_mcp.server import BizniWebMCPServer

async def test_server():
    """Test the MCP server functionality"""
    print("Testing BizniWeb MCP Server")
    print("=" * 50)
    
    # Check for API token
    if not os.getenv('BIZNISWEB_API_TOKEN'):
        print("ERROR: BIZNISWEB_API_TOKEN not found in environment")
        print("Please set it in .env file or as environment variable")
        return
    
    server = BizniWebMCPServer()
    
    # Initialize client
    try:
        await server._init_client()
        print("✓ GraphQL client initialized successfully")
    except Exception as e:
        print(f"✗ Failed to initialize client: {e}")
        return
    
    # Test 1: List recent orders
    print("\n1. Testing list_orders (last 7 days):")
    try:
        to_date = datetime.now()
        from_date = to_date - timedelta(days=7)
        result = await server._list_orders({
            'from_date': from_date.strftime('%Y-%m-%d'),
            'to_date': to_date.strftime('%Y-%m-%d'),
            'limit': 5
        })
        print(f"✓ Found {result['count']} orders")
        if result['orders']:
            print(f"  Latest order: {result['orders'][0]['order_num']} - {result['orders'][0]['customer']}")
    except Exception as e:
        print(f"✗ Error: {e}")
    
    # Test 2: Get order statistics
    print("\n2. Testing order_statistics (last 30 days):")
    try:
        result = await server._order_statistics({})
        print(f"✓ Statistics retrieved")
        print(f"  Total orders: {result['summary']['total_orders']}")
        print(f"  Total revenue: {result['summary']['total_revenue']}")
        print(f"  Average order value: {result['summary']['average_order_value']}")
    except Exception as e:
        print(f"✗ Error: {e}")
    
    # Test 3: Search orders
    print("\n3. Testing search_orders:")
    try:
        result = await server._search_orders({'query': 'gmail'})
        print(f"✓ Search completed")
        print(f"  Found {result['count']} orders matching 'gmail'")
    except Exception as e:
        print(f"✗ Error: {e}")
    
    # Test 4: Get specific order (if we have any)
    print("\n4. Testing get_order:")
    try:
        # First get a list to find an order number
        list_result = await server._list_orders({'limit': 1})
        if list_result['orders']:
            order_num = list_result['orders'][0]['order_num']
            result = await server._get_order({'order_num': order_num})
            print(f"✓ Order {order_num} retrieved")
            print(f"  Customer: {result['customer']['name']}")
            print(f"  Status: {result['status']}")
            print(f"  Items: {len(result['items'])}")
        else:
            print("  No orders found to test with")
    except Exception as e:
        print(f"✗ Error: {e}")
    
    print("\n" + "=" * 50)
    print("Testing completed!")

if __name__ == "__main__":
    # Load environment variables
    from dotenv import load_dotenv
    load_dotenv()
    
    # Run tests
    asyncio.run(test_server())