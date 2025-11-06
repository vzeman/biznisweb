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
                         fb_daily_spend: Dict[str, float] = None,
                         google_ads_daily_spend: Dict[str, float] = None, 
                         returning_customers_analysis: pd.DataFrame = None,
                         clv_return_time_analysis: pd.DataFrame = None) -> str:
    """
    Generate a complete HTML report with charts and tables
    """
    
    # Prepare data for charts
    dates = date_agg['date'].astype(str).tolist()
    revenue_data = date_agg['total_revenue'].tolist()
    product_expense_data = date_agg['product_expense'].tolist()
    fb_ads_data = date_agg['fb_ads_spend'].tolist()
    google_ads_data = date_agg['google_ads_spend'].tolist() if 'google_ads_spend' in date_agg.columns else [0] * len(dates)
    profit_data = date_agg['net_profit'].tolist()
    roi_data = date_agg['roi_percent'].tolist()
    orders_data = date_agg['unique_orders'].tolist()
    
    # Calculate Average Order Value for each day
    aov_data = [(row['total_revenue'] / row['unique_orders'] if row['unique_orders'] > 0 else 0)
                for _, row in date_agg.iterrows()]

    # Calculate Average Items per Order for each day
    avg_items_per_order_data = [(row['total_items'] / row['unique_orders'] if row['unique_orders'] > 0 else 0)
                                for _, row in date_agg.iterrows()]

    # Calculate total costs for each day (for the all metrics chart)
    total_costs_data = date_agg['total_cost'].tolist()
    packaging_costs_data = date_agg['packaging_cost'].tolist()
    fixed_daily_costs_data = date_agg['fixed_daily_cost'].tolist()
    items_data = date_agg['total_items'].tolist()
    
    # Calculate totals
    total_revenue = date_agg['total_revenue'].sum()
    total_product_expense = date_agg['product_expense'].sum()
    total_packaging = date_agg['packaging_cost'].sum()
    total_fixed = date_agg['fixed_daily_cost'].sum()
    total_fixed_costs = total_packaging + total_fixed
    total_fb_ads = date_agg['fb_ads_spend'].sum()
    total_google_ads = date_agg['google_ads_spend'].sum() if 'google_ads_spend' in date_agg.columns else 0
    total_cost = date_agg['total_cost'].sum()
    total_profit = date_agg['net_profit'].sum()
    total_roi = (total_profit / total_cost * 100) if total_cost > 0 else 0
    total_orders = date_agg['unique_orders'].sum()
    total_items = date_agg['total_items'].sum()
    total_aov = total_revenue / total_orders if total_orders > 0 else 0
    total_fb_per_order = total_fb_ads / total_orders if total_orders > 0 else 0
    total_avg_items_per_order = total_items / total_orders if total_orders > 0 else 0
    
    # All products sorted by revenue
    all_products = items_agg.sort_values('total_revenue', ascending=False)

    # Calculate totals for share percentages
    total_all_products_quantity = all_products['total_quantity'].sum()
    total_all_products_revenue = all_products['total_revenue'].sum()
    
    # Prepare returning customers data if available
    returning_html = ""
    returning_chart_js = ""
    
    if returning_customers_analysis is not None and not returning_customers_analysis.empty:
        # Prepare data for returning customers chart
        weeks = returning_customers_analysis['week'].astype(str).tolist()
        week_starts = returning_customers_analysis['week_start'].astype(str).tolist()
        returning_pct = returning_customers_analysis['returning_percentage'].tolist()
        new_pct = returning_customers_analysis['new_percentage'].tolist()
        returning_orders = returning_customers_analysis['returning_orders'].tolist()
        new_orders = returning_customers_analysis['new_orders'].tolist()
        total_orders_weekly = returning_customers_analysis['total_orders'].tolist()
        unique_customers = returning_customers_analysis['unique_customers'].tolist()
        
        # Calculate totals for returning customers
        total_returning = returning_customers_analysis['returning_orders'].sum()
        total_new = returning_customers_analysis['new_orders'].sum()
        total_weekly_orders = returning_customers_analysis['total_orders'].sum()
        overall_returning_pct = (total_returning / total_weekly_orders * 100) if total_weekly_orders > 0 else 0
        overall_new_pct = (total_new / total_weekly_orders * 100) if total_weekly_orders > 0 else 0
        
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
                <div class="card-title">Fixed Costs</div>
                <div class="card-value cost">€{total_fixed_costs:,.2f}</div>
            </div>
            <div class="card">
                <div class="card-title">Facebook Ads</div>
                <div class="card-value cost">€{total_fb_ads:,.2f}</div>
            </div>
            <div class="card">
                <div class="card-title">Google Ads</div>
                <div class="card-value cost">€{total_google_ads:,.2f}</div>
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
            <div class="card">
                <div class="card-title">Avg Order Value</div>
                <div class="card-value">€{total_aov:.2f}</div>
            </div>
            <div class="card">
                <div class="card-title">Avg FB Cost/Order</div>
                <div class="card-value cost">€{total_fb_per_order:.2f}</div>
            </div>"""
    
    # Add returning customers card if data is available
    if returning_customers_analysis is not None and not returning_customers_analysis.empty:
        html_content += f"""
            <div class="card">
                <div class="card-title">Returning Customers</div>
                <div class="card-value roi">{overall_returning_pct:.1f}%</div>
            </div>"""
    
    # Add CLV and CAC cards if data is available
    if clv_return_time_analysis is not None and not clv_return_time_analysis.empty:
        # Get the final cumulative CLV which represents the overall average
        overall_clv = clv_return_time_analysis['cumulative_avg_clv'].iloc[-1]
        
        # Calculate overall CAC
        total_fb_spend = clv_return_time_analysis['fb_ads_spend'].sum() if 'fb_ads_spend' in clv_return_time_analysis.columns else 0
        total_new_customers = clv_return_time_analysis['new_customers'].sum()
        overall_cac = total_fb_spend / total_new_customers if total_new_customers > 0 else 0
        
        html_content += f"""
            <div class="card">
                <div class="card-title">Avg Customer LTV</div>
                <div class="card-value">€{overall_clv:.2f}</div>
            </div>
            <div class="card">
                <div class="card-title">Customer Acq. Cost</div>
                <div class="card-value cost">€{overall_cac:.2f}</div>
            </div>
            <div class="card">
                <div class="card-title">LTV/CAC Ratio</div>
                <div class="card-value roi">{overall_clv / overall_cac if overall_cac > 0 else 0:.2f}x</div>
            </div>"""
    
    html_content += """
        </div>
        
        <div class="chart-container">
            <h2 class="chart-title">Daily Revenue vs Costs</h2>
            <canvas id="revenueChart"></canvas>
        </div>
        
        <div class="chart-container">
            <h2 class="chart-title">All Metrics Overview</h2>
            <canvas id="allMetricsChart"></canvas>
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
        
        <h2 style="text-align: center; color: white; margin: 40px 0 20px; font-size: 2rem;">Individual Metrics</h2>
        
        <div class="chart-grid">
            <div class="chart-container">
                <h2 class="chart-title">Daily Revenue</h2>
                <canvas id="revenueOnlyChart"></canvas>
            </div>
            <div class="chart-container">
                <h2 class="chart-title">Daily Total Costs</h2>
                <canvas id="totalCostsChart"></canvas>
            </div>
        </div>
        
        <div class="chart-grid">
            <div class="chart-container">
                <h2 class="chart-title">Daily Product Costs</h2>
                <canvas id="productCostsChart"></canvas>
            </div>
            <div class="chart-container">
                <h2 class="chart-title">Daily Facebook Ads</h2>
                <canvas id="fbAdsChart"></canvas>
            </div>
        </div>
        
        <div class="chart-grid">
            <div class="chart-container">
                <h2 class="chart-title">Daily Google Ads</h2>
                <canvas id="googleAdsChart"></canvas>
            </div>
            <div class="chart-container">
                <h2 class="chart-title">Ads Comparison (FB vs Google)</h2>
                <canvas id="adsComparisonChart"></canvas>
            </div>
        </div>
        
        <div class="chart-grid">
            <div class="chart-container">
                <h2 class="chart-title">Daily Packaging Costs</h2>
                <canvas id="packagingCostsChart"></canvas>
            </div>
            <div class="chart-container">
                <h2 class="chart-title">Daily Fixed Costs</h2>
                <canvas id="fixedCostsChart"></canvas>
            </div>
        </div>
        
        <div class="chart-grid">
            <div class="chart-container">
                <h2 class="chart-title">Daily Average Order Value</h2>
                <canvas id="aovChart"></canvas>
            </div>
            <div class="chart-container">
                <h2 class="chart-title">Daily Items Sold</h2>
                <canvas id="itemsChart"></canvas>
            </div>
        </div>

        <div class="chart-container">
            <h2 class="chart-title">Average Items per Order</h2>
            <canvas id="avgItemsPerOrderChart"></canvas>
        </div>"""
    
    # Add returning customers charts and table if data is available
    if returning_customers_analysis is not None and not returning_customers_analysis.empty:
        html_content += f"""
        
        <h2 style="text-align: center; color: white; margin: 40px 0 20px; font-size: 2rem;">Customer Retention Analysis</h2>
        
        <div class="chart-grid">
            <div class="chart-container">
                <h2 class="chart-title">Customer Type Distribution (Weekly %)</h2>
                <canvas id="returningPctChart"></canvas>
            </div>
            <div class="chart-container">
                <h2 class="chart-title">Order Volume by Customer Type (Weekly)</h2>
                <canvas id="returningVolumeChart"></canvas>
            </div>
        </div>
        
        <div class="table-container">
            <h2 class="table-title">Weekly Customer Retention Analysis</h2>
            <table>
                <thead>
                    <tr>
                        <th>Week</th>
                        <th>Week Start</th>
                        <th class="number">Total Orders</th>
                        <th class="number">New Orders</th>
                        <th class="number">New %</th>
                        <th class="number">Returning Orders</th>
                        <th class="number">Returning %</th>
                        <th class="number">Unique Customers</th>
                    </tr>
                </thead>
                <tbody>"""
        
        # Add weekly rows
        for i, row in returning_customers_analysis.iterrows():
            returning_class = "profit-positive" if row['returning_percentage'] > 10 else ""
            html_content += f"""
                    <tr>
                        <td>{row['week']}</td>
                        <td>{row['week_start']}</td>
                        <td class="number">{row['total_orders']}</td>
                        <td class="number">{row['new_orders']}</td>
                        <td class="number">{row['new_percentage']:.1f}%</td>
                        <td class="number {returning_class}">{row['returning_orders']}</td>
                        <td class="number {returning_class}">{row['returning_percentage']:.1f}%</td>
                        <td class="number">{row['unique_customers']}</td>
                    </tr>"""
        
        # Add total row
        html_content += f"""
                    <tr class="total-row">
                        <td colspan="2">TOTAL</td>
                        <td class="number">{total_weekly_orders}</td>
                        <td class="number">{total_new}</td>
                        <td class="number">{overall_new_pct:.1f}%</td>
                        <td class="number">{total_returning}</td>
                        <td class="number">{overall_returning_pct:.1f}%</td>
                        <td class="number">{returning_customers_analysis['unique_customers'].sum()}</td>
                    </tr>
                </tbody>
            </table>
        </div>"""
    
    # Add CLV and return time analysis if data is available
    if clv_return_time_analysis is not None and not clv_return_time_analysis.empty:
        # Prepare data for CLV charts
        clv_weeks = clv_return_time_analysis['week'].astype(str).tolist()
        clv_week_starts = clv_return_time_analysis['week_start'].astype(str).tolist()
        avg_clv = clv_return_time_analysis['avg_clv'].tolist()
        cumulative_clv = clv_return_time_analysis['cumulative_avg_clv'].tolist()
        avg_return_days = clv_return_time_analysis['avg_return_time_days'].fillna(0).tolist()
        clv_new_customers = clv_return_time_analysis['new_customers'].tolist()
        clv_returning_customers = clv_return_time_analysis['returning_customers'].tolist()
        cac_data = clv_return_time_analysis['cac'].tolist() if 'cac' in clv_return_time_analysis.columns else [0] * len(clv_weeks)
        ltv_cac_ratio_data = clv_return_time_analysis['ltv_cac_ratio'].tolist() if 'ltv_cac_ratio' in clv_return_time_analysis.columns else [0] * len(clv_weeks)
        
        # Calculate overall metrics
        overall_avg_clv = clv_return_time_analysis['avg_clv'].mean()
        final_cumulative_clv = clv_return_time_analysis['cumulative_avg_clv'].iloc[-1] if not clv_return_time_analysis.empty else 0
        overall_avg_return = clv_return_time_analysis['avg_return_time_days'].mean()
        
        html_content += f"""
        
        <h2 style="text-align: center; color: white; margin: 40px 0 20px; font-size: 2rem;">Customer Lifetime Value, CAC & Return Time Analysis</h2>
        
        <div class="chart-grid">
            <div class="chart-container">
                <h2 class="chart-title">Customer Lifetime Value Trend (Weekly)</h2>
                <canvas id="clvChart"></canvas>
            </div>
            <div class="chart-container">
                <h2 class="chart-title">Customer Acquisition Cost Trend (Weekly)</h2>
                <canvas id="cacChart"></canvas>
            </div>
        </div>
        
        <div class="chart-grid">
            <div class="chart-container">
                <h2 class="chart-title">CLV vs CAC Comparison</h2>
                <canvas id="clvCacComparisonChart"></canvas>
            </div>
            <div class="chart-container">
                <h2 class="chart-title">LTV/CAC Ratio Trend</h2>
                <canvas id="ltvCacRatioChart"></canvas>
            </div>
        </div>
        
        <div class="chart-container">
            <h2 class="chart-title">Average Customer Return Time (Days)</h2>
            <canvas id="returnTimeChart"></canvas>
        </div>
        
        <div class="table-container">
            <h2 class="table-title">Weekly CLV, CAC & Return Time Analysis</h2>
            <table>
                <thead>
                    <tr>
                        <th>Week</th>
                        <th>Week Start</th>
                        <th class="number">Customers</th>
                        <th class="number">New</th>
                        <th class="number">Returning</th>
                        <th class="number">Avg CLV (€)</th>
                        <th class="number">Cumulative CLV (€)</th>
                        <th class="number">CAC (€)</th>
                        <th class="number">Avg Return Days</th>
                        <th class="number">Revenue (€)</th>
                    </tr>
                </thead>
                <tbody>"""
        
        # Add weekly rows
        for i, row in clv_return_time_analysis.iterrows():
            return_time_str = f"{row['avg_return_time_days']:.1f}" if pd.notna(row['avg_return_time_days']) else "N/A"
            cac = row.get('cac', 0)
            html_content += f"""
                    <tr>
                        <td>{row['week']}</td>
                        <td>{row['week_start']}</td>
                        <td class="number">{row['unique_customers']}</td>
                        <td class="number">{row['new_customers']}</td>
                        <td class="number">{row['returning_customers']}</td>
                        <td class="number">€{row['avg_clv']:.2f}</td>
                        <td class="number">€{row['cumulative_avg_clv']:.2f}</td>
                        <td class="number">€{cac:.2f}</td>
                        <td class="number">{return_time_str}</td>
                        <td class="number">€{row['total_revenue']:.2f}</td>
                    </tr>"""
        
        # Add total row
        total_customers = clv_return_time_analysis['unique_customers'].sum()
        total_new = clv_return_time_analysis['new_customers'].sum()
        total_returning = clv_return_time_analysis['returning_customers'].sum()
        total_revenue = clv_return_time_analysis['total_revenue'].sum()
        return_time_total = f"{overall_avg_return:.1f}" if pd.notna(overall_avg_return) else "N/A"
        
        # Calculate overall CAC for the total row
        total_fb_spend_table = clv_return_time_analysis['fb_ads_spend'].sum() if 'fb_ads_spend' in clv_return_time_analysis.columns else 0
        overall_cac_table = total_fb_spend_table / total_new if total_new > 0 else 0
        
        html_content += f"""
                    <tr class="total-row">
                        <td colspan="2">TOTAL/AVG</td>
                        <td class="number">{total_customers}</td>
                        <td class="number">{total_new}</td>
                        <td class="number">{total_returning}</td>
                        <td class="number">€{overall_avg_clv:.2f}</td>
                        <td class="number">€{final_cumulative_clv:.2f}</td>
                        <td class="number">€{overall_cac_table:.2f}</td>
                        <td class="number">{return_time_total}</td>
                        <td class="number">€{total_revenue:.2f}</td>
                    </tr>
                </tbody>
            </table>
        </div>"""
    
    html_content += """
        
        <div class="table-container">
            <h2 class="table-title">Daily Performance Summary</h2>
            <table>
                <thead>
                    <tr>
                        <th>Date</th>
                        <th class="number">Orders</th>
                        <th class="number">Revenue</th>
                        <th class="number">AOV</th>
                        <th class="number">Avg Items/Order</th>
                        <th class="number">Product Costs</th>
                        <th class="number">Fixed Costs</th>
                        <th class="number">FB Ads</th>
                        <th class="number">Google Ads</th>
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
        fixed_costs = row['packaging_cost'] + row['fixed_daily_cost']
        aov = row['total_revenue'] / row['unique_orders'] if row['unique_orders'] > 0 else 0
        avg_items_per_order = row['total_items'] / row['unique_orders'] if row['unique_orders'] > 0 else 0
        fb_per_order = row['fb_ads_spend'] / row['unique_orders'] if row['unique_orders'] > 0 else 0
        google_ads = row.get('google_ads_spend', 0)
        html_content += f"""
                    <tr>
                        <td>{row['date']}</td>
                        <td class="number">{row['unique_orders']}</td>
                        <td class="number">€{row['total_revenue']:,.2f}</td>
                        <td class="number">€{aov:.2f}</td>
                        <td class="number">{avg_items_per_order:.2f}</td>
                        <td class="number">€{row['product_expense']:,.2f}</td>
                        <td class="number">€{fixed_costs:,.2f}</td>
                        <td class="number">€{row['fb_ads_spend']:,.2f}</td>
                        <td class="number">€{google_ads:,.2f}</td>
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
                        <td class="number">€{total_aov:.2f}</td>
                        <td class="number">{total_avg_items_per_order:.2f}</td>
                        <td class="number">€{total_product_expense:,.2f}</td>
                        <td class="number">€{total_fixed_costs:,.2f}</td>
                        <td class="number">€{total_fb_ads:,.2f}</td>
                        <td class="number">€{total_google_ads:,.2f}</td>
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
                        <th class="number">Share (Items Sold / Revenue)</th>
                    </tr>
                </thead>
                <tbody>
"""
    
    # Add all products
    for _, row in all_products.iterrows():
        profit_class = "profit-positive" if row['profit'] > 0 else "profit-negative"
        product_name = row['product_name'][:50] + '...' if len(row['product_name']) > 50 else row['product_name']

        # Calculate share percentages
        quantity_share = (row['total_quantity'] / total_all_products_quantity * 100) if total_all_products_quantity > 0 else 0
        revenue_share = (row['total_revenue'] / total_all_products_revenue * 100) if total_all_products_revenue > 0 else 0

        html_content += f"""
                    <tr>
                        <td>{product_name}</td>
                        <td class="number">{row['total_quantity']}</td>
                        <td class="number">€{row['total_revenue']:,.2f}</td>
                        <td class="number">€{row['product_expense']:,.2f}</td>
                        <td class="number {profit_class}">€{row['profit']:,.2f}</td>
                        <td class="number">{row['roi_percent']:.1f}%</td>
                        <td class="number">{quantity_share:.1f}% / {revenue_share:.1f}%</td>
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
                    }},
                    {{
                        label: 'Google Ads',
                        data: {json.dumps(google_ads_data)},
                        borderColor: '#34D399',
                        backgroundColor: 'rgba(52, 211, 153, 0.1)',
                        borderWidth: 2,
                        tension: 0.4
                    }},
                    {{
                        label: 'Net Profit',
                        data: {json.dumps(profit_data)},
                        borderColor: '#9f7aea',
                        backgroundColor: 'rgba(159, 122, 234, 0.1)',
                        borderWidth: 3,
                        tension: 0.4,
                        borderDash: [5, 5]
                    }},
                    {{
                        label: 'Avg Order Value',
                        data: {json.dumps(aov_data)},
                        borderColor: '#f687b3',
                        backgroundColor: 'rgba(246, 135, 179, 0.1)',
                        borderWidth: 2,
                        tension: 0.4,
                        yAxisID: 'y1',
                        borderDash: [2, 2]
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
                        type: 'linear',
                        display: true,
                        position: 'left',
                        beginAtZero: true,
                        ticks: {{
                            callback: function(value) {{
                                return '€' + value.toFixed(0);
                            }}
                        }}
                    }},
                    y1: {{
                        type: 'linear',
                        display: true,
                        position: 'right',
                        beginAtZero: true,
                        grid: {{
                            drawOnChartArea: false,
                        }},
                        ticks: {{
                            callback: function(value) {{
                                return '€' + value.toFixed(0);
                            }}
                        }}
                    }}
                }}
            }}
        }});
        
        // All Metrics Chart
        const allMetricsCtx = document.getElementById('allMetricsChart').getContext('2d');
        new Chart(allMetricsCtx, {{
            type: 'line',
            data: {{
                labels: {json.dumps(dates)},
                datasets: [
                    {{
                        label: 'Revenue',
                        data: {json.dumps(revenue_data)},
                        borderColor: '#48bb78',
                        backgroundColor: 'rgba(72, 187, 120, 0.1)',
                        borderWidth: 2,
                        tension: 0.4
                    }},
                    {{
                        label: 'Total Costs',
                        data: {json.dumps(total_costs_data)},
                        borderColor: '#f56565',
                        backgroundColor: 'rgba(245, 101, 101, 0.1)',
                        borderWidth: 2,
                        tension: 0.4
                    }},
                    {{
                        label: 'Product Costs',
                        data: {json.dumps(product_expense_data)},
                        borderColor: '#ed8936',
                        backgroundColor: 'rgba(237, 137, 54, 0.1)',
                        borderWidth: 2,
                        tension: 0.4,
                        hidden: false
                    }},
                    {{
                        label: 'Facebook Ads',
                        data: {json.dumps(fb_ads_data)},
                        borderColor: '#4299e1',
                        backgroundColor: 'rgba(66, 153, 225, 0.1)',
                        borderWidth: 2,
                        tension: 0.4,
                        hidden: false
                    }},
                    {{
                        label: 'Google Ads',
                        data: {json.dumps(google_ads_data)},
                        borderColor: '#34D399',
                        backgroundColor: 'rgba(52, 211, 153, 0.1)',
                        borderWidth: 2,
                        tension: 0.4,
                        hidden: false
                    }},
                    {{
                        label: 'Packaging Costs',
                        data: {json.dumps(packaging_costs_data)},
                        borderColor: '#38b2ac',
                        backgroundColor: 'rgba(56, 178, 172, 0.1)',
                        borderWidth: 2,
                        tension: 0.4,
                        hidden: false
                    }},
                    {{
                        label: 'Fixed Daily Costs',
                        data: {json.dumps(fixed_daily_costs_data)},
                        borderColor: '#805ad5',
                        backgroundColor: 'rgba(128, 90, 213, 0.1)',
                        borderWidth: 2,
                        tension: 0.4,
                        hidden: false
                    }},
                    {{
                        label: 'Net Profit',
                        data: {json.dumps(profit_data)},
                        borderColor: '#9f7aea',
                        backgroundColor: 'rgba(159, 122, 234, 0.1)',
                        borderWidth: 3,
                        tension: 0.4
                    }},
                    {{
                        label: 'Avg Order Value',
                        data: {json.dumps(aov_data)},
                        borderColor: '#f687b3',
                        backgroundColor: 'rgba(246, 135, 179, 0.1)',
                        borderWidth: 2,
                        tension: 0.4,
                        yAxisID: 'y1'
                    }},
                    {{
                        label: 'ROI %',
                        data: {json.dumps(roi_data)},
                        borderColor: '#667eea',
                        backgroundColor: 'rgba(102, 126, 234, 0.1)',
                        borderWidth: 2,
                        tension: 0.4,
                        yAxisID: 'y2'
                    }}
                ]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: true,
                aspectRatio: 2.5,
                interaction: {{
                    mode: 'index',
                    intersect: false,
                }},
                plugins: {{
                    legend: {{
                        position: 'top',
                        labels: {{
                            usePointStyle: true,
                            padding: 15
                        }}
                    }},
                    tooltip: {{
                        mode: 'index',
                        intersect: false,
                        callbacks: {{
                            label: function(context) {{
                                let label = context.dataset.label + ': ';
                                if (context.dataset.label === 'ROI %') {{
                                    return label + context.parsed.y.toFixed(1) + '%';
                                }}
                                return label + '€' + context.parsed.y.toFixed(2);
                            }}
                        }}
                    }}
                }},
                scales: {{
                    y: {{
                        type: 'linear',
                        display: true,
                        position: 'left',
                        beginAtZero: true,
                        title: {{
                            display: true,
                            text: 'Amount (€)'
                        }},
                        ticks: {{
                            callback: function(value) {{
                                return '€' + value.toFixed(0);
                            }}
                        }}
                    }},
                    y1: {{
                        type: 'linear',
                        display: true,
                        position: 'right',
                        beginAtZero: true,
                        title: {{
                            display: true,
                            text: 'AOV (€)'
                        }},
                        grid: {{
                            drawOnChartArea: false,
                        }},
                        ticks: {{
                            callback: function(value) {{
                                return '€' + value.toFixed(0);
                            }}
                        }}
                    }},
                    y2: {{
                        type: 'linear',
                        display: true,
                        position: 'right',
                        beginAtZero: true,
                        title: {{
                            display: true,
                            text: 'ROI %'
                        }},
                        grid: {{
                            drawOnChartArea: false,
                        }},
                        ticks: {{
                            callback: function(value) {{
                                return value.toFixed(0) + '%';
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
                labels: ['Product Costs', 'Fixed Costs', 'Facebook Ads', 'Google Ads'],
                datasets: [{{
                    data: [{total_product_expense:.2f}, {total_fixed_costs:.2f}, {total_fb_ads:.2f}, {total_google_ads:.2f}],
                    backgroundColor: ['#ed8936', '#48bb78', '#4299e1', '#34D399'],
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
        
        // Individual Metric Charts
        
        // Revenue Only Chart
        const revenueOnlyCtx = document.getElementById('revenueOnlyChart').getContext('2d');
        new Chart(revenueOnlyCtx, {{
            type: 'line',
            data: {{
                labels: {json.dumps(dates)},
                datasets: [{{
                    label: 'Revenue',
                    data: {json.dumps(revenue_data)},
                    borderColor: '#48bb78',
                    backgroundColor: 'rgba(72, 187, 120, 0.2)',
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
                    legend: {{ display: false }},
                    tooltip: {{
                        callbacks: {{
                            label: function(context) {{
                                return 'Revenue: €' + context.parsed.y.toFixed(2);
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
        
        // Total Costs Chart
        const totalCostsCtx = document.getElementById('totalCostsChart').getContext('2d');
        new Chart(totalCostsCtx, {{
            type: 'line',
            data: {{
                labels: {json.dumps(dates)},
                datasets: [{{
                    label: 'Total Costs',
                    data: {json.dumps(total_costs_data)},
                    borderColor: '#f56565',
                    backgroundColor: 'rgba(245, 101, 101, 0.2)',
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
                    legend: {{ display: false }},
                    tooltip: {{
                        callbacks: {{
                            label: function(context) {{
                                return 'Total Costs: €' + context.parsed.y.toFixed(2);
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
        
        // Product Costs Chart
        const productCostsCtx = document.getElementById('productCostsChart').getContext('2d');
        new Chart(productCostsCtx, {{
            type: 'bar',
            data: {{
                labels: {json.dumps(dates)},
                datasets: [{{
                    label: 'Product Costs',
                    data: {json.dumps(product_expense_data)},
                    backgroundColor: '#ed8936',
                    borderRadius: 5
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: true,
                aspectRatio: 2,
                plugins: {{
                    legend: {{ display: false }},
                    tooltip: {{
                        callbacks: {{
                            label: function(context) {{
                                return 'Product Costs: €' + context.parsed.y.toFixed(2);
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
        
        // Facebook Ads Chart
        const fbAdsCtx = document.getElementById('fbAdsChart').getContext('2d');
        new Chart(fbAdsCtx, {{
            type: 'bar',
            data: {{
                labels: {json.dumps(dates)},
                datasets: [{{
                    label: 'Facebook Ads',
                    data: {json.dumps(fb_ads_data)},
                    backgroundColor: '#4299e1',
                    borderRadius: 5
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: true,
                aspectRatio: 2,
                plugins: {{
                    legend: {{ display: false }},
                    tooltip: {{
                        callbacks: {{
                            label: function(context) {{
                                return 'FB Ads: €' + context.parsed.y.toFixed(2);
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
        
        // Google Ads Chart
        const googleAdsCtx = document.getElementById('googleAdsChart').getContext('2d');
        new Chart(googleAdsCtx, {{
            type: 'bar',
            data: {{
                labels: {json.dumps(dates)},
                datasets: [{{
                    label: 'Google Ads',
                    data: {json.dumps(google_ads_data)},
                    backgroundColor: '#34D399',
                    borderRadius: 5
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: true,
                aspectRatio: 2,
                plugins: {{
                    legend: {{ display: false }},
                    tooltip: {{
                        callbacks: {{
                            label: function(context) {{
                                return 'Google Ads: €' + context.parsed.y.toFixed(2);
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
        
        // Ads Comparison Chart
        const adsComparisonCtx = document.getElementById('adsComparisonChart').getContext('2d');
        new Chart(adsComparisonCtx, {{
            type: 'bar',
            data: {{
                labels: {json.dumps(dates)},
                datasets: [
                    {{
                        label: 'Facebook Ads',
                        data: {json.dumps(fb_ads_data)},
                        backgroundColor: '#4299e1',
                        borderRadius: 5
                    }},
                    {{
                        label: 'Google Ads',
                        data: {json.dumps(google_ads_data)},
                        backgroundColor: '#34D399',
                        borderRadius: 5
                    }}
                ]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: true,
                aspectRatio: 2,
                plugins: {{
                    legend: {{
                        position: 'top'
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
        
        // Packaging Costs Chart
        const packagingCostsCtx = document.getElementById('packagingCostsChart').getContext('2d');
        new Chart(packagingCostsCtx, {{
            type: 'bar',
            data: {{
                labels: {json.dumps(dates)},
                datasets: [{{
                    label: 'Packaging Costs',
                    data: {json.dumps(packaging_costs_data)},
                    backgroundColor: '#38b2ac',
                    borderRadius: 5
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: true,
                aspectRatio: 2,
                plugins: {{
                    legend: {{ display: false }},
                    tooltip: {{
                        callbacks: {{
                            label: function(context) {{
                                return 'Packaging: €' + context.parsed.y.toFixed(2);
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
        
        // Fixed Costs Chart
        const fixedCostsCtx = document.getElementById('fixedCostsChart').getContext('2d');
        new Chart(fixedCostsCtx, {{
            type: 'bar',
            data: {{
                labels: {json.dumps(dates)},
                datasets: [{{
                    label: 'Fixed Daily Costs',
                    data: {json.dumps(fixed_daily_costs_data)},
                    backgroundColor: '#805ad5',
                    borderRadius: 5
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: true,
                aspectRatio: 2,
                plugins: {{
                    legend: {{ display: false }},
                    tooltip: {{
                        callbacks: {{
                            label: function(context) {{
                                return 'Fixed Costs: €' + context.parsed.y.toFixed(2);
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
        
        // Average Order Value Chart
        const aovCtx = document.getElementById('aovChart').getContext('2d');
        new Chart(aovCtx, {{
            type: 'line',
            data: {{
                labels: {json.dumps(dates)},
                datasets: [{{
                    label: 'AOV',
                    data: {json.dumps(aov_data)},
                    borderColor: '#f687b3',
                    backgroundColor: 'rgba(246, 135, 179, 0.2)',
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
                    legend: {{ display: false }},
                    tooltip: {{
                        callbacks: {{
                            label: function(context) {{
                                return 'AOV: €' + context.parsed.y.toFixed(2);
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
        
        // Items Sold Chart
        const itemsCtx = document.getElementById('itemsChart').getContext('2d');
        new Chart(itemsCtx, {{
            type: 'bar',
            data: {{
                labels: {json.dumps(dates)},
                datasets: [{{
                    label: 'Items Sold',
                    data: {json.dumps(items_data)},
                    backgroundColor: '#fc8181',
                    borderRadius: 5
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: true,
                aspectRatio: 2,
                plugins: {{
                    legend: {{ display: false }},
                    tooltip: {{
                        callbacks: {{
                            label: function(context) {{
                                return 'Items: ' + context.parsed.y;
                            }}
                        }}
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

        // Average Items per Order Chart
        const avgItemsPerOrderCtx = document.getElementById('avgItemsPerOrderChart').getContext('2d');
        new Chart(avgItemsPerOrderCtx, {{
            type: 'line',
            data: {{
                labels: {json.dumps(dates)},
                datasets: [{{
                    label: 'Avg Items per Order',
                    data: {json.dumps(avg_items_per_order_data)},
                    borderColor: '#8b5cf6',
                    backgroundColor: 'rgba(139, 92, 246, 0.2)',
                    borderWidth: 3,
                    tension: 0.4,
                    fill: true
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: true,
                aspectRatio: 2.5,
                plugins: {{
                    legend: {{ display: false }},
                    tooltip: {{
                        callbacks: {{
                            label: function(context) {{
                                return 'Avg Items/Order: ' + context.parsed.y.toFixed(2);
                            }}
                        }}
                    }}
                }},
                scales: {{
                    y: {{
                        beginAtZero: true,
                        ticks: {{
                            callback: function(value) {{
                                return value.toFixed(1);
                            }}
                        }}
                    }}
                }}
            }}
        }});"""
    
    # Add JavaScript for returning customers charts if data is available
    if returning_customers_analysis is not None and not returning_customers_analysis.empty:
        html_content += f"""
        
        // Returning Customers Percentage Chart
        const returningPctCtx = document.getElementById('returningPctChart');
        if (returningPctCtx) {{
            new Chart(returningPctCtx.getContext('2d'), {{
                type: 'line',
                data: {{
                    labels: {json.dumps(week_starts)},
                    datasets: [
                        {{
                            label: 'Returning Customers %',
                            data: {json.dumps(returning_pct)},
                            borderColor: '#2E86AB',
                            backgroundColor: 'rgba(46, 134, 171, 0.1)',
                            borderWidth: 3,
                            tension: 0.4,
                            fill: true
                        }},
                        {{
                            label: 'New Customers %',
                            data: {json.dumps(new_pct)},
                            borderColor: '#A23B72',
                            backgroundColor: 'rgba(162, 59, 114, 0.1)',
                            borderWidth: 3,
                            tension: 0.4,
                            fill: true
                        }}
                    ]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: true,
                    aspectRatio: 2,
                    plugins: {{
                        legend: {{
                            position: 'top'
                        }},
                        tooltip: {{
                            mode: 'index',
                            intersect: false,
                            callbacks: {{
                                label: function(context) {{
                                    return context.dataset.label + ': ' + context.parsed.y.toFixed(1) + '%';
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        y: {{
                            beginAtZero: true,
                            max: 100,
                            ticks: {{
                                callback: function(value) {{
                                    return value + '%';
                                }}
                            }}
                        }}
                    }}
                }}
            }});
        }}
        
        // Returning Customers Volume Chart
        const returningVolumeCtx = document.getElementById('returningVolumeChart');
        if (returningVolumeCtx) {{
            new Chart(returningVolumeCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(week_starts)},
                    datasets: [
                        {{
                            label: 'New Customer Orders',
                            data: {json.dumps(new_orders)},
                            backgroundColor: '#A23B72',
                            borderRadius: 5
                        }},
                        {{
                            label: 'Returning Customer Orders',
                            data: {json.dumps(returning_orders)},
                            backgroundColor: '#2E86AB',
                            borderRadius: 5
                        }}
                    ]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: true,
                    aspectRatio: 2,
                    plugins: {{
                        legend: {{
                            position: 'top'
                        }},
                        tooltip: {{
                            mode: 'index',
                            intersect: false
                        }}
                    }},
                    scales: {{
                        x: {{
                            stacked: true
                        }},
                        y: {{
                            stacked: true,
                            beginAtZero: true
                        }}
                    }}
                }}
            }});
        }}"""
    
    # Add JavaScript for CLV and return time charts if data is available
    if clv_return_time_analysis is not None and not clv_return_time_analysis.empty:
        html_content += f"""
        
        // Customer Lifetime Value Chart
        const clvCtx = document.getElementById('clvChart');
        if (clvCtx) {{
            new Chart(clvCtx.getContext('2d'), {{
                type: 'line',
                data: {{
                    labels: {json.dumps(clv_week_starts)},
                    datasets: [
                        {{
                            label: 'Average CLV (€)',
                            data: {json.dumps(avg_clv)},
                            borderColor: '#48bb78',
                            backgroundColor: 'rgba(72, 187, 120, 0.1)',
                            borderWidth: 3,
                            tension: 0.4,
                            yAxisID: 'y'
                        }},
                        {{
                            label: 'Cumulative Avg CLV (€)',
                            data: {json.dumps(cumulative_clv)},
                            borderColor: '#667eea',
                            backgroundColor: 'rgba(102, 126, 234, 0.1)',
                            borderWidth: 3,
                            tension: 0.4,
                            borderDash: [5, 5],
                            yAxisID: 'y'
                        }}
                    ]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: true,
                    aspectRatio: 2,
                    interaction: {{
                        mode: 'index',
                        intersect: false,
                    }},
                    plugins: {{
                        legend: {{
                            position: 'top'
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
                            type: 'linear',
                            display: true,
                            position: 'left',
                            beginAtZero: true,
                            title: {{
                                display: true,
                                text: 'CLV (€)'
                            }},
                            ticks: {{
                                callback: function(value) {{
                                    return '€' + value.toFixed(0);
                                }}
                            }}
                        }}
                    }}
                }}
            }});
        }}
        
        // Customer Acquisition Cost Chart
        const cacCtx = document.getElementById('cacChart');
        if (cacCtx) {{
            new Chart(cacCtx.getContext('2d'), {{
                type: 'line',
                data: {{
                    labels: {json.dumps(clv_week_starts)},
                    datasets: [
                        {{
                            label: 'CAC (€)',
                            data: {json.dumps(cac_data)},
                            borderColor: '#f56565',
                            backgroundColor: 'rgba(245, 101, 101, 0.1)',
                            borderWidth: 3,
                            tension: 0.4
                        }}
                    ]
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
                                    return 'CAC: €' + context.parsed.y.toFixed(2);
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        y: {{
                            beginAtZero: true,
                            title: {{
                                display: true,
                                text: 'CAC (€)'
                            }},
                            ticks: {{
                                callback: function(value) {{
                                    return '€' + value.toFixed(0);
                                }}
                            }}
                        }}
                    }}
                }}
            }});
        }}
        
        // CLV vs CAC Comparison Chart
        const clvCacComparisonCtx = document.getElementById('clvCacComparisonChart');
        if (clvCacComparisonCtx) {{
            new Chart(clvCacComparisonCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(clv_week_starts)},
                    datasets: [
                        {{
                            label: 'CLV (€)',
                            data: {json.dumps(avg_clv)},
                            backgroundColor: '#48bb78',
                            borderRadius: 5
                        }},
                        {{
                            label: 'CAC (€)',
                            data: {json.dumps(cac_data)},
                            backgroundColor: '#f56565',
                            borderRadius: 5
                        }}
                    ]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: true,
                    aspectRatio: 2,
                    plugins: {{
                        legend: {{
                            position: 'top'
                        }},
                        tooltip: {{
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
                            title: {{
                                display: true,
                                text: 'Amount (€)'
                            }},
                            ticks: {{
                                callback: function(value) {{
                                    return '€' + value.toFixed(0);
                                }}
                            }}
                        }}
                    }}
                }}
            }});
        }}
        
        // LTV/CAC Ratio Chart
        const ltvCacRatioCtx = document.getElementById('ltvCacRatioChart');
        if (ltvCacRatioCtx) {{
            new Chart(ltvCacRatioCtx.getContext('2d'), {{
                type: 'line',
                data: {{
                    labels: {json.dumps(clv_week_starts)},
                    datasets: [
                        {{
                            label: 'LTV/CAC Ratio',
                            data: {json.dumps(ltv_cac_ratio_data)},
                            borderColor: '#9f7aea',
                            backgroundColor: 'rgba(159, 122, 234, 0.1)',
                            borderWidth: 3,
                            tension: 0.4,
                            fill: true
                        }},
                        {{
                            label: 'Break-even Line (1.0)',
                            data: Array({len(clv_week_starts)}).fill(1),
                            borderColor: '#718096',
                            borderWidth: 2,
                            borderDash: [10, 5],
                            fill: false,
                            pointRadius: 0
                        }},
                        {{
                            label: 'Target Line (3.0)',
                            data: Array({len(clv_week_starts)}).fill(3),
                            borderColor: '#48bb78',
                            borderWidth: 2,
                            borderDash: [10, 5],
                            fill: false,
                            pointRadius: 0
                        }}
                    ]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: true,
                    aspectRatio: 2,
                    plugins: {{
                        legend: {{
                            position: 'top'
                        }},
                        tooltip: {{
                            callbacks: {{
                                label: function(context) {{
                                    if (context.dataset.label.includes('Line')) {{
                                        return context.dataset.label;
                                    }}
                                    return context.dataset.label + ': ' + context.parsed.y.toFixed(2) + 'x';
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        y: {{
                            beginAtZero: true,
                            title: {{
                                display: true,
                                text: 'LTV/CAC Ratio'
                            }},
                            ticks: {{
                                callback: function(value) {{
                                    return value.toFixed(1) + 'x';
                                }}
                            }}
                        }}
                    }}
                }}
            }});
        }}
        
        // Customer Return Time Chart
        const returnTimeCtx = document.getElementById('returnTimeChart');
        if (returnTimeCtx) {{
            new Chart(returnTimeCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(clv_week_starts)},
                    datasets: [
                        {{
                            label: 'Average Return Time (Days)',
                            data: {json.dumps(avg_return_days)},
                            backgroundColor: '#ed8936',
                            borderRadius: 5
                        }}
                    ]
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
                                    if (context.parsed.y === 0) {{
                                        return 'No returning customers';
                                    }}
                                    return 'Avg Return: ' + context.parsed.y.toFixed(1) + ' days';
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        y: {{
                            beginAtZero: true,
                            title: {{
                                display: true,
                                text: 'Days'
                            }},
                            ticks: {{
                                callback: function(value) {{
                                    return value.toFixed(0);
                                }}
                            }}
                        }}
                    }}
                }}
            }});
        }}"""
    
    html_content += """
    </script>
</body>
</html>
"""
    
    return html_content