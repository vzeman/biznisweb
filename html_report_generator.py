#!/usr/bin/env python3
"""
HTML Report Generator for BizniWeb Order Export
Generates beautiful HTML reports with charts and tables
"""

import pandas as pd
from datetime import datetime
from typing import Dict, Any
import json


def generate_html_report(date_agg: pd.DataFrame, date_product_agg: pd.DataFrame, 
                         items_agg: pd.DataFrame, date_from: datetime, date_to: datetime,
                         fb_daily_spend: Dict[str, float] = None) -> str:
    """
    Generate a complete HTML report with charts and tables
    """
    
    # Prepare data for charts
    dates = date_agg['date'].astype(str).tolist()
    revenue_data = date_agg['total_revenue'].tolist()
    product_expense_data = date_agg['product_expense'].tolist()
    fb_ads_data = date_agg['fb_ads_spend'].tolist()
    profit_data = date_agg['net_profit'].tolist()
    roi_data = date_agg['roi_percent'].tolist()
    orders_data = date_agg['unique_orders'].tolist()
    
    # Calculate totals
    total_revenue = date_agg['total_revenue'].sum()
    total_product_expense = date_agg['product_expense'].sum()
    total_fb_ads = date_agg['fb_ads_spend'].sum()
    total_cost = total_product_expense + total_fb_ads
    total_profit = date_agg['net_profit'].sum()
    total_roi = (total_profit / total_cost * 100) if total_cost > 0 else 0
    total_orders = date_agg['unique_orders'].sum()
    total_items = date_agg['total_items'].sum()
    
    # All products sorted by revenue
    all_products = items_agg.sort_values('total_revenue', ascending=False)
    
    html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>BizniWeb Sales Report - {date_from.strftime('%Y-%m-%d')} to {date_to.strftime('%Y-%m-%d')}</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }}
        
        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}
        
        .header {{
            background: white;
            border-radius: 15px;
            padding: 30px;
            margin-bottom: 30px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
        }}
        
        .header h1 {{
            color: #2d3748;
            margin-bottom: 10px;
            font-size: 2.5rem;
        }}
        
        .header .date-range {{
            color: #718096;
            font-size: 1.2rem;
        }}
        
        .summary-cards {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        
        .card {{
            background: white;
            border-radius: 15px;
            padding: 25px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
            transition: transform 0.3s ease;
        }}
        
        .card:hover {{
            transform: translateY(-5px);
        }}
        
        .card-title {{
            color: #718096;
            font-size: 0.9rem;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 10px;
        }}
        
        .card-value {{
            font-size: 2rem;
            font-weight: bold;
            color: #2d3748;
        }}
        
        .card-value.profit {{
            color: #48bb78;
        }}
        
        .card-value.cost {{
            color: #f56565;
        }}
        
        .card-value.roi {{
            color: #667eea;
        }}
        
        .chart-container {{
            background: white;
            border-radius: 15px;
            padding: 30px;
            margin-bottom: 30px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
            position: relative;
        }}
        
        .chart-container canvas {{
            max-height: 300px !important;
        }}
        
        .chart-title {{
            font-size: 1.5rem;
            color: #2d3748;
            margin-bottom: 20px;
            text-align: center;
        }}
        
        .chart-grid {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 30px;
            margin-bottom: 30px;
        }}
        
        @media (max-width: 768px) {{
            .chart-grid {{
                grid-template-columns: 1fr;
            }}
        }}
        
        .table-container {{
            background: white;
            border-radius: 15px;
            padding: 30px;
            margin-bottom: 30px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
            overflow-x: auto;
        }}
        
        .table-title {{
            font-size: 1.5rem;
            color: #2d3748;
            margin-bottom: 20px;
        }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
        }}
        
        th {{
            background: #f7fafc;
            color: #4a5568;
            font-weight: 600;
            text-align: left;
            padding: 15px;
            border-bottom: 2px solid #e2e8f0;
        }}
        
        td {{
            padding: 15px;
            border-bottom: 1px solid #e2e8f0;
            color: #2d3748;
        }}
        
        tr:hover {{
            background: #f7fafc;
        }}
        
        .number {{
            text-align: right;
            font-variant-numeric: tabular-nums;
        }}
        
        .footer {{
            text-align: center;
            color: white;
            padding: 20px;
            font-size: 0.9rem;
        }}
        
        .profit-positive {{
            color: #48bb78;
            font-weight: 600;
        }}
        
        .profit-negative {{
            color: #f56565;
            font-weight: 600;
        }}
        
        .total-row {{
            background: #f7fafc;
            font-weight: bold;
        }}
        
        .total-row td {{
            border-top: 2px solid #4a5568;
            border-bottom: 2px solid #4a5568;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 BizniWeb Sales Report</h1>
            <div class="date-range">{date_from.strftime('%B %d, %Y')} - {date_to.strftime('%B %d, %Y')}</div>
        </div>
        
        <div class="summary-cards">
            <div class="card">
                <div class="card-title">Total Revenue</div>
                <div class="card-value">€{total_revenue:,.2f}</div>
            </div>
            <div class="card">
                <div class="card-title">Product Costs</div>
                <div class="card-value cost">€{total_product_expense:,.2f}</div>
            </div>
            <div class="card">
                <div class="card-title">Facebook Ads</div>
                <div class="card-value cost">€{total_fb_ads:,.2f}</div>
            </div>
            <div class="card">
                <div class="card-title">Total Costs</div>
                <div class="card-value cost">€{total_cost:,.2f}</div>
            </div>
            <div class="card">
                <div class="card-title">Net Profit</div>
                <div class="card-value profit">€{total_profit:,.2f}</div>
            </div>
            <div class="card">
                <div class="card-title">ROI</div>
                <div class="card-value roi">{total_roi:.1f}%</div>
            </div>
            <div class="card">
                <div class="card-title">Total Orders</div>
                <div class="card-value">{total_orders}</div>
            </div>
            <div class="card">
                <div class="card-title">Total Items</div>
                <div class="card-value">{total_items}</div>
            </div>
        </div>
        
        <div class="chart-container">
            <h2 class="chart-title">Daily Revenue vs Costs</h2>
            <canvas id="revenueChart"></canvas>
        </div>
        
        <div class="chart-grid">
            <div class="chart-container">
                <h2 class="chart-title">Daily Profit</h2>
                <canvas id="profitChart"></canvas>
            </div>
            <div class="chart-container">
                <h2 class="chart-title">Daily ROI %</h2>
                <canvas id="roiChart"></canvas>
            </div>
        </div>
        
        <div class="chart-grid">
            <div class="chart-container">
                <h2 class="chart-title">Cost Breakdown</h2>
                <canvas id="costPieChart"></canvas>
            </div>
            <div class="chart-container">
                <h2 class="chart-title">Daily Orders</h2>
                <canvas id="ordersChart"></canvas>
            </div>
        </div>
        
        <div class="table-container">
            <h2 class="table-title">Daily Performance Summary</h2>
            <table>
                <thead>
                    <tr>
                        <th>Date</th>
                        <th class="number">Orders</th>
                        <th class="number">Revenue</th>
                        <th class="number">Product Costs</th>
                        <th class="number">FB Ads</th>
                        <th class="number">Total Costs</th>
                        <th class="number">Profit</th>
                        <th class="number">ROI %</th>
                    </tr>
                </thead>
                <tbody>
"""
    
    # Add daily rows
    for _, row in date_agg.iterrows():
        profit_class = "profit-positive" if row['net_profit'] > 0 else "profit-negative"
        html_content += f"""
                    <tr>
                        <td>{row['date']}</td>
                        <td class="number">{row['unique_orders']}</td>
                        <td class="number">€{row['total_revenue']:,.2f}</td>
                        <td class="number">€{row['product_expense']:,.2f}</td>
                        <td class="number">€{row['fb_ads_spend']:,.2f}</td>
                        <td class="number">€{row['total_cost']:,.2f}</td>
                        <td class="number {profit_class}">€{row['net_profit']:,.2f}</td>
                        <td class="number">{row['roi_percent']:.1f}%</td>
                    </tr>
"""
    
    # Add total row
    html_content += f"""
                    <tr class="total-row">
                        <td>TOTAL</td>
                        <td class="number">{total_orders}</td>
                        <td class="number">€{total_revenue:,.2f}</td>
                        <td class="number">€{total_product_expense:,.2f}</td>
                        <td class="number">€{total_fb_ads:,.2f}</td>
                        <td class="number">€{total_cost:,.2f}</td>
                        <td class="number profit-positive">€{total_profit:,.2f}</td>
                        <td class="number">{total_roi:.1f}%</td>
                    </tr>
                </tbody>
            </table>
        </div>
        
        <div class="table-container">
            <h2 class="table-title">All Products by Revenue (Product Costs Only)</h2>
            <table>
                <thead>
                    <tr>
                        <th>Product</th>
                        <th class="number">Quantity</th>
                        <th class="number">Revenue</th>
                        <th class="number">Product Cost</th>
                        <th class="number">Profit</th>
                        <th class="number">ROI %</th>
                    </tr>
                </thead>
                <tbody>
"""
    
    # Add all products
    for _, row in all_products.iterrows():
        profit_class = "profit-positive" if row['profit'] > 0 else "profit-negative"
        product_name = row['product_name'][:50] + '...' if len(row['product_name']) > 50 else row['product_name']
        html_content += f"""
                    <tr>
                        <td>{product_name}</td>
                        <td class="number">{row['total_quantity']}</td>
                        <td class="number">€{row['total_revenue']:,.2f}</td>
                        <td class="number">€{row['product_expense']:,.2f}</td>
                        <td class="number {profit_class}">€{row['profit']:,.2f}</td>
                        <td class="number">{row['roi_percent']:.1f}%</td>
                    </tr>
"""
    
    html_content += f"""
                </tbody>
            </table>
        </div>
        
        <div class="footer">
            Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | BizniWeb Order Export System
        </div>
    </div>
    
    <script>
        // Chart defaults
        Chart.defaults.font.family = '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif';
        
        // Revenue vs Costs Chart
        const revenueCtx = document.getElementById('revenueChart').getContext('2d');
        new Chart(revenueCtx, {{
            type: 'line',
            data: {{
                labels: {json.dumps(dates)},
                datasets: [
                    {{
                        label: 'Revenue',
                        data: {json.dumps(revenue_data)},
                        borderColor: '#48bb78',
                        backgroundColor: 'rgba(72, 187, 120, 0.1)',
                        borderWidth: 3,
                        tension: 0.4
                    }},
                    {{
                        label: 'Product Costs',
                        data: {json.dumps(product_expense_data)},
                        borderColor: '#ed8936',
                        backgroundColor: 'rgba(237, 137, 54, 0.1)',
                        borderWidth: 2,
                        tension: 0.4
                    }},
                    {{
                        label: 'Facebook Ads',
                        data: {json.dumps(fb_ads_data)},
                        borderColor: '#4299e1',
                        backgroundColor: 'rgba(66, 153, 225, 0.1)',
                        borderWidth: 2,
                        tension: 0.4
                    }}
                ]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: true,
                aspectRatio: 2.5,
                plugins: {{
                    legend: {{
                        position: 'top',
                    }},
                    tooltip: {{
                        mode: 'index',
                        intersect: false,
                        callbacks: {{
                            label: function(context) {{
                                return context.dataset.label + ': €' + context.parsed.y.toFixed(2);
                            }}
                        }}
                    }}
                }},
                scales: {{
                    y: {{
                        beginAtZero: true,
                        ticks: {{
                            callback: function(value) {{
                                return '€' + value.toFixed(0);
                            }}
                        }}
                    }}
                }}
            }}
        }});
        
        // Profit Chart
        const profitCtx = document.getElementById('profitChart').getContext('2d');
        new Chart(profitCtx, {{
            type: 'bar',
            data: {{
                labels: {json.dumps(dates)},
                datasets: [{{
                    label: 'Net Profit',
                    data: {json.dumps(profit_data)},
                    backgroundColor: {json.dumps(['#48bb78' if p > 0 else '#f56565' for p in profit_data])},
                    borderRadius: 5
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: true,
                aspectRatio: 2,
                plugins: {{
                    legend: {{
                        display: false
                    }},
                    tooltip: {{
                        callbacks: {{
                            label: function(context) {{
                                return 'Profit: €' + context.parsed.y.toFixed(2);
                            }}
                        }}
                    }}
                }},
                scales: {{
                    y: {{
                        beginAtZero: true,
                        ticks: {{
                            callback: function(value) {{
                                return '€' + value.toFixed(0);
                            }}
                        }}
                    }}
                }}
            }}
        }});
        
        // ROI Chart
        const roiCtx = document.getElementById('roiChart').getContext('2d');
        new Chart(roiCtx, {{
            type: 'line',
            data: {{
                labels: {json.dumps(dates)},
                datasets: [{{
                    label: 'ROI %',
                    data: {json.dumps(roi_data)},
                    borderColor: '#667eea',
                    backgroundColor: 'rgba(102, 126, 234, 0.1)',
                    borderWidth: 3,
                    tension: 0.4,
                    fill: true
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: true,
                aspectRatio: 2,
                plugins: {{
                    legend: {{
                        display: false
                    }},
                    tooltip: {{
                        callbacks: {{
                            label: function(context) {{
                                return 'ROI: ' + context.parsed.y.toFixed(1) + '%';
                            }}
                        }}
                    }}
                }},
                scales: {{
                    y: {{
                        beginAtZero: true,
                        ticks: {{
                            callback: function(value) {{
                                return value.toFixed(0) + '%';
                            }}
                        }}
                    }}
                }}
            }}
        }});
        
        // Cost Breakdown Pie Chart
        const costPieCtx = document.getElementById('costPieChart').getContext('2d');
        new Chart(costPieCtx, {{
            type: 'doughnut',
            data: {{
                labels: ['Product Costs', 'Facebook Ads'],
                datasets: [{{
                    data: [{total_product_expense:.2f}, {total_fb_ads:.2f}],
                    backgroundColor: ['#ed8936', '#4299e1'],
                    borderWidth: 2,
                    borderColor: '#fff'
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: true,
                aspectRatio: 2,
                plugins: {{
                    legend: {{
                        position: 'bottom'
                    }},
                    tooltip: {{
                        callbacks: {{
                            label: function(context) {{
                                const percentage = (context.parsed / {total_cost:.2f} * 100).toFixed(1);
                                return context.label + ': €' + context.parsed.toFixed(2) + ' (' + percentage + '%)';
                            }}
                        }}
                    }}
                }}
            }}
        }});
        
        // Orders Chart
        const ordersCtx = document.getElementById('ordersChart').getContext('2d');
        new Chart(ordersCtx, {{
            type: 'bar',
            data: {{
                labels: {json.dumps(dates)},
                datasets: [{{
                    label: 'Orders',
                    data: {json.dumps(orders_data)},
                    backgroundColor: '#9f7aea',
                    borderRadius: 5
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: true,
                aspectRatio: 2,
                plugins: {{
                    legend: {{
                        display: false
                    }}
                }},
                scales: {{
                    y: {{
                        beginAtZero: true,
                        ticks: {{
                            stepSize: 10
                        }}
                    }}
                }}
            }}
        }});
    </script>
</body>
</html>
"""
    
    return html_content