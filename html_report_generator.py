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
                         clv_return_time_analysis: pd.DataFrame = None,
                         order_size_distribution: pd.DataFrame = None,
                         item_combinations: pd.DataFrame = None,
                         day_of_week_analysis: pd.DataFrame = None,
                         day_hour_heatmap: pd.DataFrame = None,
                         country_analysis: pd.DataFrame = None,
                         city_analysis: pd.DataFrame = None,
                         geo_profitability: dict = None,
                         b2b_analysis: pd.DataFrame = None,
                         product_margins: pd.DataFrame = None,
                         product_trends: pd.DataFrame = None,
                         customer_concentration: dict = None,
                         financial_metrics: dict = None,
                         order_status: pd.DataFrame = None,
                         ads_effectiveness: dict = None,
                         new_vs_returning_revenue: dict = None,
                         refunds_analysis: dict = None,
                         customer_email_segments: dict = None,
                         cohort_analysis: dict = None,
                         first_item_retention: dict = None,
                         same_item_repurchase: dict = None,
                         time_to_nth_by_first_item: dict = None,
                         fb_detailed_metrics: dict = None,
                         fb_campaigns: list = None,
                         cost_per_order: dict = None,
                         fb_hourly_stats: list = None,
                         fb_dow_stats: list = None,
                         ltv_by_date: pd.DataFrame = None,
                         consistency_checks: dict = None) -> str:
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
    product_gross_margin_daily_data = [
        ((row['total_revenue'] - row['product_expense']) / row['total_revenue'] * 100) if row['total_revenue'] > 0 else 0
        for _, row in date_agg.iterrows()
    ]

    # Calculate Average Items per Order for each day
    avg_items_per_order_data = [(row['total_items'] / row['unique_orders'] if row['unique_orders'] > 0 else 0)
                                for _, row in date_agg.iterrows()]
    post_ad_contribution_per_order_data = (
        date_agg['contribution_profit_per_order'].tolist()
        if 'contribution_profit_per_order' in date_agg.columns
        else [
            (row['net_profit'] / row['unique_orders'] if row['unique_orders'] > 0 else 0)
            for _, row in date_agg.iterrows()
        ]
    )
    pre_ad_contribution_per_order_data = (
        date_agg['pre_ad_contribution_profit_per_order'].tolist()
        if 'pre_ad_contribution_profit_per_order' in date_agg.columns
        else [
            (
                (row['total_revenue'] - row['product_expense'] - row['packaging_cost'] - row.get('shipping_subsidy_cost', 0))
                / row['unique_orders']
                if row['unique_orders'] > 0 else 0
            )
            for _, row in date_agg.iterrows()
        ]
    )

    # Running (cumulative) daily averages to visualize trend in time
    cumulative_avg_revenue_data = []
    cumulative_avg_profit_data = []
    running_revenue = 0
    running_profit = 0
    for idx, (daily_revenue, daily_profit) in enumerate(zip(revenue_data, profit_data), 1):
        running_revenue += daily_revenue
        running_profit += daily_profit
        cumulative_avg_revenue_data.append(running_revenue / idx)
        cumulative_avg_profit_data.append(running_profit / idx)

    # Calculate total costs for each day (for the all metrics chart)
    total_costs_data = date_agg['total_cost'].tolist()
    packaging_costs_data = date_agg['packaging_cost'].tolist()
    shipping_subsidy_data = date_agg['shipping_subsidy_cost'].tolist() if 'shipping_subsidy_cost' in date_agg.columns else [0] * len(dates)
    fixed_daily_costs_data = date_agg['fixed_daily_cost'].tolist()
    items_data = date_agg['total_items'].tolist()

    # Prepare LTV by acquisition date data
    ltv_revenue_data = []
    ltv_dates = []
    if ltv_by_date is not None and not ltv_by_date.empty:
        ltv_dates = ltv_by_date['date'].astype(str).tolist()
        ltv_revenue_data = ltv_by_date['ltv_revenue'].tolist()
    else:
        # If no LTV data, use same dates as regular data with zeros
        ltv_dates = dates
        ltv_revenue_data = [0] * len(dates)

    # Calculate LTV-based daily profit (LTV Revenue - Total Costs)
    ltv_profit_data = [ltv_rev - cost for ltv_rev, cost in zip(ltv_revenue_data, total_costs_data)]

    # Calculate totals
    total_revenue = date_agg['total_revenue'].sum()
    total_product_expense = date_agg['product_expense'].sum()
    total_packaging = date_agg['packaging_cost'].sum()
    total_shipping_subsidy = date_agg['shipping_subsidy_cost'].sum() if 'shipping_subsidy_cost' in date_agg.columns else 0
    total_fixed = date_agg['fixed_daily_cost'].sum()
    total_fixed_costs = total_packaging + total_shipping_subsidy + total_fixed
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
    total_days = len(date_agg.index)
    avg_daily_revenue = total_revenue / total_days if total_days > 0 else 0
    avg_daily_profit = total_profit / total_days if total_days > 0 else 0

    new_ret_dates = []
    new_ret_new_revenue = []
    new_ret_returning_revenue = []
    new_ret_summary = {}
    if new_vs_returning_revenue and new_vs_returning_revenue.get('daily') is not None and not new_vs_returning_revenue.get('daily').empty:
        new_ret_daily = new_vs_returning_revenue['daily']
        new_ret_dates = new_ret_daily['date'].astype(str).tolist()
        new_ret_new_revenue = new_ret_daily['new_revenue'].tolist()
        new_ret_returning_revenue = new_ret_daily['returning_revenue'].tolist()
        new_ret_summary = new_vs_returning_revenue.get('summary', {})

    refunds_dates = []
    refunds_rate = []
    refunds_amount = []
    if refunds_analysis and refunds_analysis.get('daily') is not None and not refunds_analysis.get('daily').empty:
        refunds_daily = refunds_analysis['daily']
        refunds_dates = refunds_daily['date'].astype(str).tolist()
        refunds_rate = refunds_daily['refund_rate_pct'].tolist()
        refunds_amount = refunds_daily['refund_amount'].tolist()
    
    # All products sorted by revenue
    all_products = items_agg.sort_values('total_revenue', ascending=False)

    # Calculate totals for share percentages
    total_all_products_quantity = all_products['total_quantity'].sum()
    total_all_products_revenue = all_products['total_revenue'].sum()
    total_all_products_profit = all_products['profit'].sum()

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
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
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
            margin-bottom: 10px;
            text-align: center;
        }}

        .chart-explanation {{
            font-size: 0.85rem;
            color: #718096;
            margin-bottom: 20px;
            text-align: center;
            padding: 0 20px;
            line-height: 1.5;
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

        /* Collapsible table styles */
        .collapsible-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            cursor: pointer;
            padding: 15px 0;
            user-select: none;
        }}

        .collapsible-header:hover {{
            opacity: 0.8;
        }}

        .collapsible-header .toggle-icon {{
            font-size: 1.5rem;
            color: #667eea;
            transition: transform 0.3s ease;
            margin-left: 15px;
        }}

        .collapsible-header.expanded .toggle-icon {{
            transform: rotate(180deg);
        }}

        .collapsible-content {{
            max-height: 0;
            overflow: hidden;
            transition: max-height 0.3s ease-out;
        }}

        .collapsible-content.expanded {{
            max-height: none;
        }}

        .table-container .table-title {{
            margin-bottom: 0;
        }}

        .expand-all-btn {{
            background: #667eea;
            color: white;
            border: none;
            padding: 10px 20px;
            border-radius: 8px;
            cursor: pointer;
            font-size: 0.9rem;
            margin-bottom: 20px;
            transition: background 0.3s ease;
        }}

        .expand-all-btn:hover {{
            background: #5a67d8;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>BizniWeb Sales Report</h1>
            <div class="date-range">{date_from.strftime('%B %d, %Y')} - {date_to.strftime('%B %d, %Y')}</div>
            <button id="toggleAllBtn" class="expand-all-btn" onclick="toggleAllTables(true)" style="margin-top: 15px;">Expand All Tables</button>
        </div>
        
        <div class="summary-cards">
            <div class="card">
                <div class="card-title">Total Revenue (Net)</div>
                <div class="card-value">€{total_revenue:,.2f}</div>
            </div>
            <div class="card">
                <div class="card-title">Product Costs</div>
                <div class="card-value cost">€{total_product_expense:,.2f}</div>
            </div>
            <div class="card">
                <div class="card-title">Packaging Costs</div>
                <div class="card-value cost">&#8364;{total_packaging:,.2f}</div>
            </div>
            <div class="card">
                <div class="card-title">Shipping Subsidy</div>
                <div class="card-value cost">&#8364;{total_shipping_subsidy:,.2f}</div>
            </div>
            <div class="card">
                <div class="card-title">Fixed Overhead</div>
                <div class="card-value cost">&#8364;{total_fixed:,.2f}</div>
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
                <div class="card-value {'profit' if total_profit >= 0 else 'cost'}">€{total_profit:,.2f}</div>
            </div>
            <div class="card">
                <div class="card-title">Avg Daily Revenue</div>
                <div class="card-value">€{avg_daily_revenue:,.2f}</div>
            </div>
            <div class="card">
                <div class="card-title">Avg Daily Profit/Loss</div>
                <div class="card-value {'profit' if avg_daily_profit >= 0 else 'cost'}">€{avg_daily_profit:,.2f}</div>
            </div>
            <div class="card">
                <div class="card-title">ROI</div>
                <div class="card-value {'profit' if total_roi >= 0 else 'cost'}">{total_roi:.1f}%</div>
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
                <div class="card-title">Avg Customer LTV (Revenue)</div>
                <div class="card-value">€{overall_clv:.2f}</div>
                <div style="color: #718096; font-size: 0.8rem;">Realized revenue/customer in selected interval</div>
            </div>
            <div class="card">
                <div class="card-title">Customer Acq. Cost (FB)</div>
                <div class="card-value cost">€{overall_cac:.2f}</div>
            </div>
            <div class="card">
                <div class="card-title">Revenue LTV/CAC</div>
                <div class="card-value {'profit' if (overall_clv / overall_cac if overall_cac > 0 else 0) > 1 else 'cost'}">{overall_clv / overall_cac if overall_cac > 0 else 0:.2f}x</div>
                <div style="color: #718096; font-size: 0.8rem;">Revenue-based ratio</div>
            </div>"""

    # Add financial metrics cards if available
    if financial_metrics:
        roas = financial_metrics.get('roas', 0)
        mer = financial_metrics.get('mer', roas)
        revenue_per_customer = financial_metrics.get('revenue_per_customer', 0)
        orders_per_customer = financial_metrics.get('orders_per_customer', 0)
        product_gross_margin = financial_metrics.get('product_gross_margin_pct', 0)
        company_profit_margin = financial_metrics.get('company_profit_margin_pct', financial_metrics.get('profit_margin_pct', 0))
        pre_ad_contribution_margin = financial_metrics.get('pre_ad_contribution_margin_pct', 0)
        pre_ad_contribution_profit = financial_metrics.get('pre_ad_contribution_profit', 0)
        pre_ad_contribution_per_order = financial_metrics.get('pre_ad_contribution_profit_per_order', financial_metrics.get('pre_ad_contribution_per_order', 0))
        post_ad_contribution_margin = financial_metrics.get('post_ad_contribution_margin_pct', financial_metrics.get('contribution_margin_pct', 0))
        post_ad_contribution_profit = financial_metrics.get('post_ad_contribution_profit', financial_metrics.get('contribution_profit', 0))
        post_ad_contribution_per_order = financial_metrics.get('post_ad_contribution_profit_per_order', financial_metrics.get('contribution_profit_per_order', 0))
        break_even_cac = financial_metrics.get('break_even_cac', 0)
        break_even_cac_order_based = financial_metrics.get('break_even_cac_order_based', pre_ad_contribution_per_order)
        pre_ad_contribution_per_customer = financial_metrics.get('pre_ad_contribution_per_customer', 0)
        current_fb_cac = financial_metrics.get('current_fb_cac', 0)
        paid_cac = financial_metrics.get('paid_cac', current_fb_cac)
        blended_cac = financial_metrics.get('blended_cac', current_fb_cac)
        blended_cac_scope = financial_metrics.get('blended_cac_scope', 'tracked_ads_fb_google')
        cac_headroom = financial_metrics.get('cac_headroom', 0)
        contribution_ltv_cac = financial_metrics.get('contribution_ltv_cac', 0)
        cac_vs_break_even = (current_fb_cac / break_even_cac) if break_even_cac > 0 else 0
        new_revenue = financial_metrics.get('new_revenue', 0)
        returning_revenue = financial_metrics.get('returning_revenue', 0)
        new_revenue_share_pct = financial_metrics.get('new_revenue_share_pct', 0)
        returning_revenue_share_pct = financial_metrics.get('returning_revenue_share_pct', 0)
        payback_orders = financial_metrics.get('payback_orders', None)
        payback_days_estimated = financial_metrics.get('payback_days_estimated', None)
        post_ad_payback_orders = financial_metrics.get('post_ad_payback_orders', None)
        post_ad_payback_days_estimated = financial_metrics.get('post_ad_payback_days_estimated', None)
        payback_days_note = financial_metrics.get('payback_days_note', '')
        payback_orders_display = f"{payback_orders:.2f} orders" if payback_orders is not None else "N/A"
        payback_days_display = f"{payback_days_estimated:.0f} days" if payback_days_estimated is not None else "N/A"
        post_ad_payback_orders_display = f"{post_ad_payback_orders:.2f} orders" if post_ad_payback_orders is not None else "N/A"
        post_ad_payback_days_display = f"{post_ad_payback_days_estimated:.0f} days" if post_ad_payback_days_estimated is not None else "N/A"
        blended_cac_hint = "FB+Google" if blended_cac_scope == "tracked_ads_fb_google" else "tracked channels"
        html_content += f"""
            <div class="card">
                <div class="card-title">ROAS (All Ads)</div>
                <div class="card-value {'profit' if roas > 1 else 'cost'}">{roas:.2f}x</div>
            </div>
            <div class="card">
                <div class="card-title">MER</div>
                <div class="card-value {'profit' if mer > 1 else 'cost'}">{mer:.2f}x</div>
            </div>
            <div class="card">
                <div class="card-title">Revenue/Customer (Net)</div>
                <div class="card-value">&#8364;{revenue_per_customer:.2f}</div>
            </div>
            <div class="card">
                <div class="card-title">Orders / Customer</div>
                <div class="card-value">{orders_per_customer:.2f}</div>
            </div>
            <div class="card">
                <div class="card-title">Company Profit Margin</div>
                <div class="card-value {'profit' if company_profit_margin > 0 else 'cost'}">{company_profit_margin:.1f}%</div>
                <div style="color: #718096; font-size: 0.8rem;">Includes fixed overhead</div>
            </div>
            <div class="card">
                <div class="card-title">Product Gross Margin</div>
                <div class="card-value {'profit' if product_gross_margin > 0 else 'cost'}">{product_gross_margin:.1f}%</div>
            </div>
            <div class="card">
                <div class="card-title">Pre-Ad Contribution Profit</div>
                <div class="card-value {'profit' if pre_ad_contribution_profit > 0 else 'cost'}">&#8364;{pre_ad_contribution_profit:,.2f}</div>
            </div>
            <div class="card">
                <div class="card-title">Pre-Ad Contribution Margin</div>
                <div class="card-value {'profit' if pre_ad_contribution_margin > 0 else 'cost'}">{pre_ad_contribution_margin:.1f}%</div>
            </div>
            <div class="card">
                <div class="card-title">Pre-Ad Contribution / Order</div>
                <div class="card-value {'profit' if pre_ad_contribution_per_order > 0 else 'cost'}">&#8364;{pre_ad_contribution_per_order:,.2f}</div>
                <div style="color: #718096; font-size: 0.8rem;">Base for payback (order-level)</div>
            </div>
            <div class="card">
                <div class="card-title">Post-Ad Contribution Profit</div>
                <div class="card-value {'profit' if post_ad_contribution_profit > 0 else 'cost'}">&#8364;{post_ad_contribution_profit:,.2f}</div>
                <div style="color: #718096; font-size: 0.8rem;">Excludes fixed overhead</div>
            </div>
            <div class="card">
                <div class="card-title">Post-Ad Contribution Margin</div>
                <div class="card-value {'profit' if post_ad_contribution_margin > 0 else 'cost'}">{post_ad_contribution_margin:.1f}%</div>
            </div>
            <div class="card">
                <div class="card-title">Post-Ad Contribution / Order</div>
                <div class="card-value {'profit' if post_ad_contribution_per_order > 0 else 'cost'}">&#8364;{post_ad_contribution_per_order:,.2f}</div>
            </div>
            <div class="card">
                <div class="card-title">Break-even CAC</div>
                <div class="card-value {'profit' if break_even_cac > 0 else 'cost'}">&#8364;{break_even_cac:,.2f}</div>
                <div style="color: #718096; font-size: 0.8rem;">Customer-based (pre-ad contribution/customer)</div>
                <div style="color: #718096; font-size: 0.8rem;">Order-based ref: &#8364;{break_even_cac_order_based:,.2f}/order</div>
            </div>
            <div class="card">
                <div class="card-title">Pre-Ad Contribution / Customer</div>
                <div class="card-value {'profit' if pre_ad_contribution_per_customer > 0 else 'cost'}">&#8364;{pre_ad_contribution_per_customer:,.2f}</div>
            </div>
            <div class="card">
                <div class="card-title">Current FB CAC</div>
                <div class="card-value {'profit' if break_even_cac > 0 and current_fb_cac <= break_even_cac else 'cost'}">&#8364;{current_fb_cac:,.2f}</div>
            </div>
            <div class="card">
                <div class="card-title">Paid CAC (FB)</div>
                <div class="card-value {'profit' if break_even_cac > 0 and paid_cac <= break_even_cac else 'cost'}">&#8364;{paid_cac:,.2f}</div>
            </div>
            <div class="card">
                <div class="card-title">Blended CAC (Tracked Ads)</div>
                <div class="card-value {'profit' if break_even_cac > 0 and blended_cac <= break_even_cac else 'cost'}">&#8364;{blended_cac:,.2f}</div>
                <div style="color: #718096; font-size: 0.8rem;">{blended_cac_hint}</div>
            </div>
            <div class="card">
                <div class="card-title">CAC Headroom</div>
                <div class="card-value {'profit' if cac_headroom >= 0 else 'cost'}">&#8364;{cac_headroom:+,.2f}</div>
            </div>
            <div class="card">
                <div class="card-title">CAC / Break-even</div>
                <div class="card-value {'profit' if break_even_cac > 0 and cac_vs_break_even <= 1 else 'cost'}">{cac_vs_break_even:.2f}x</div>
            </div>
            <div class="card">
                <div class="card-title">Contribution LTV/CAC</div>
                <div class="card-value {'profit' if contribution_ltv_cac > 1 else 'cost'}">{contribution_ltv_cac:.2f}x</div>
                <div style="color: #718096; font-size: 0.8rem;">Pre-ad contribution/customer ÷ FB CAC</div>
            </div>
            <div class="card">
                <div class="card-title">New Cust. Revenue</div>
                <div class="card-value">&#8364;{new_revenue:,.2f}</div>
                <div style="color: #718096; font-size: 0.8rem;">{new_revenue_share_pct:.1f}% share</div>
            </div>
            <div class="card">
                <div class="card-title">Returning Cust. Revenue</div>
                <div class="card-value">&#8364;{returning_revenue:,.2f}</div>
                <div style="color: #718096; font-size: 0.8rem;">{returning_revenue_share_pct:.1f}% share</div>
            </div>
            <div class="card">
                <div class="card-title">Payback Period (Orders)</div>
                <div class="card-value {'profit' if payback_orders is not None and payback_orders <= 1 else 'cost'}">{payback_orders_display}</div>
            </div>
            <div class="card">
                <div class="card-title">Payback Period (Days est.)</div>
                <div class="card-value {'profit' if payback_days_estimated is not None and payback_days_estimated <= 30 else 'cost'}">{payback_days_display}</div>
                <div style="color: #718096; font-size: 0.8rem;">0 days = recovered on first order contribution</div>
            </div>
            <div class="card">
                <div class="card-title">Post-Ad Payback (Orders est.)</div>
                <div class="card-value {'profit' if post_ad_payback_orders is not None and post_ad_payback_orders <= 1 else 'cost'}">{post_ad_payback_orders_display}</div>
                <div style="color: #718096; font-size: 0.8rem;">Uses post-ad contribution per order</div>
            </div>
            <div class="card">
                <div class="card-title">Post-Ad Payback (Days est.)</div>
                <div class="card-value {'profit' if post_ad_payback_days_estimated is not None and post_ad_payback_days_estimated <= 30 else 'cost'}">{post_ad_payback_days_display}</div>
                <div style="color: #718096; font-size: 0.8rem;">Estimated from average return cycle</div>
            </div>"""

    if consistency_checks:
        roas_delta = consistency_checks.get('roas_delta', 0)
        margin_delta = consistency_checks.get('company_margin_delta_pct', 0)
        cac_expected = consistency_checks.get('cac_expected', 0)
        cac_delta = consistency_checks.get('cac_delta', 0)
        cac_if_orders = consistency_checks.get('cac_if_orders_denominator', 0)
        html_content += f"""
            <div class="card">
                <div class="card-title">ROAS Check &#916;</div>
                <div class="card-value {'profit' if abs(roas_delta) <= 0.01 else 'cost'}">{roas_delta:+.4f}</div>
            </div>
            <div class="card">
                <div class="card-title">Margin Check &#916; (pp)</div>
                <div class="card-value {'profit' if abs(margin_delta) <= 0.05 else 'cost'}">{margin_delta:+.4f}</div>
            </div>
            <div class="card">
                <div class="card-title">CAC (FB/New Cust.)</div>
                <div class="card-value">€{cac_expected:.2f}</div>
            </div>
            <div class="card">
                <div class="card-title">CAC Check &#916;</div>
                <div class="card-value {'profit' if abs(cac_delta) <= 0.01 else 'cost'}">{cac_delta:+.4f}</div>
            </div>
            <div class="card">
                <div class="card-title">FB Spend / Orders</div>
                <div class="card-value cost">€{cac_if_orders:.2f}</div>
            </div>"""

    if refunds_analysis and refunds_analysis.get('summary'):
        refund_summary = refunds_analysis['summary']
        html_content += f"""
            <div class="card">
                <div class="card-title">Refund Orders</div>
                <div class="card-value cost">{refund_summary.get('refund_orders', 0)}</div>
            </div>
            <div class="card">
                <div class="card-title">Refund Rate</div>
                <div class="card-value {'cost' if refund_summary.get('refund_rate_pct', 0) > 1 else 'profit'}">{refund_summary.get('refund_rate_pct', 0):.2f}%</div>
            </div>
            <div class="card">
                <div class="card-title">Refund Amount</div>
                <div class="card-value cost">€{refund_summary.get('refund_amount', 0):,.2f}</div>
            </div>"""

    # Add customer concentration if available
    if customer_concentration:
        repeat_rate = customer_concentration.get('repeat_purchase_rate', 0)
        html_content += f"""
            <div class="card">
                <div class="card-title">Repeat Purchase Rate</div>
                <div class="card-value roi">{repeat_rate:.1f}%</div>
            </div>"""

    html_content += """
        </div>
        """

    if financial_metrics:
        html_content += """
        <div class="chart-container">
            <h2 class="chart-title">CAC vs Break-even Comparison</h2>
            <p class="chart-explanation">Compares acquisition cost thresholds on customer-level units: Paid CAC (Facebook), Blended CAC (tracked ads: FB+Google), and Break-even CAC based on pre-ad contribution per customer. Values below break-even are generally healthier for scalable growth.</p>
            <canvas id="cacComparisonChart"></canvas>
        </div>
        """

    html_content += """
        
        <div class="chart-container">
            <h2 class="chart-title">Daily Revenue vs Costs</h2>
            <p class="chart-explanation">Revenue = net sales income (without VAT) | Product Costs = cost of goods sold | FB Ads = Facebook advertising spend | Google Ads = Google advertising spend | Packaging = per-order packaging cost | Shipping Subsidy = per-order postal subsidy | Fixed Overhead = daily fixed operational cost | Net Profit = Revenue - (Product + Packaging + Shipping Subsidy + Fixed + Ads) | AOV = Average Order Value (Revenue / Orders)</p>
            <canvas id="revenueChart"></canvas>
        </div>

        <div class="chart-container">
            <h2 class="chart-title">Revenue vs Total Costs</h2>
            <p class="chart-explanation">Simple comparison of daily Revenue (green) vs Total Costs (red) as line chart</p>
            <canvas id="revenueTotalCostsChart"></canvas>
        </div>

        <div class="chart-container">
            <h2 class="chart-title">Customer Lifetime Revenue by Acquisition Date</h2>
            <p class="chart-explanation">This chart shows the total lifetime value of customers acquired on each day. For each customer's first purchase date, we sum all their revenue across all orders (including future purchases). This helps identify which acquisition dates brought the most valuable customers. Actual Daily Revenue (light blue) vs Full Customer Lifetime Revenue (dark blue) vs Total Costs (red). Compare LTV to costs to see if acquisition days were profitable long-term.</p>
            <canvas id="ltvByAcquisitionChart"></canvas>
        </div>

        <div class="chart-container">
            <h2 class="chart-title">Daily Profit (LTV-Based)</h2>
            <p class="chart-explanation">This shows profit calculated using Customer Lifetime Revenue instead of daily revenue. Formula: Full Customer Lifetime Revenue - Total Costs. Positive values (green) indicate acquisition days where customers' total lifetime value exceeded all costs incurred that day. This metric shows the true long-term profitability of customer acquisition efforts.</p>
            <canvas id="ltvProfitChart"></canvas>
        </div>

        <div class="chart-container">
            <h2 class="chart-title">All Metrics Overview</h2>
            <p class="chart-explanation">Comprehensive view of all daily metrics: Revenue, Total Costs (all expenses combined), Product Costs, Facebook Ads, Google Ads, Packaging Costs, Shipping Subsidy, Fixed Daily Costs, Net Profit, AOV (Average Order Value), and ROI % (Return on Investment percentage)</p>
            <canvas id="allMetricsChart"></canvas>
        </div>
        
        <div class="chart-grid">
            <div class="chart-container">
                <h2 class="chart-title">Daily Profit</h2>
                <p class="chart-explanation">Net Profit = Revenue - Total Costs (includes all product, fixed, packaging, shipping subsidy, and advertising costs)</p>
                <canvas id="profitChart"></canvas>
            </div>
            <div class="chart-container">
                <h2 class="chart-title">Daily ROI %</h2>
                <p class="chart-explanation">ROI (Return on Investment) = (Net Profit / Total Costs) × 100. Measures profitability as percentage of total investment</p>
                <canvas id="roiChart"></canvas>
            </div>
        </div>
        
        <div class="chart-grid">
            <div class="chart-container">
                <h2 class="chart-title">Cost Breakdown</h2>
                <p class="chart-explanation">Distribution of total costs across categories: Product Costs (COGS), Packaging Costs, Shipping Subsidy, Fixed Overhead, Facebook Ads, and Google Ads</p>
                <canvas id="costPieChart"></canvas>
            </div>
            <div class="chart-container">
                <h2 class="chart-title">Daily Orders</h2>
                <p class="chart-explanation">Number of unique orders placed each day</p>
                <canvas id="ordersChart"></canvas>
            </div>
        </div>
        
        <h2 style="text-align: center; color: white; margin: 40px 0 20px; font-size: 2rem;">Individual Metrics</h2>
        
        <div class="chart-grid">
            <div class="chart-container">
                <h2 class="chart-title">Daily Revenue</h2>
                <p class="chart-explanation">Total sales revenue (gross income before costs)</p>
                <canvas id="revenueOnlyChart"></canvas>
            </div>
            <div class="chart-container">
                <h2 class="chart-title">Daily Total Costs</h2>
                <p class="chart-explanation">Sum of all expenses: Product Costs + Packaging + Shipping Subsidy + Fixed Overhead + Facebook Ads + Google Ads</p>
                <canvas id="totalCostsChart"></canvas>
            </div>
        </div>
        
        <div class="chart-grid">
            <div class="chart-container">
                <h2 class="chart-title">Daily Product Costs</h2>
                <p class="chart-explanation">COGS (Cost of Goods Sold) - the purchase/production cost of products sold</p>
                <canvas id="productCostsChart"></canvas>
            </div>
            <div class="chart-container">
                <h2 class="chart-title">Daily Product Gross Margin %</h2>
                <p class="chart-explanation">Gross margin on products only = (Revenue - Product Costs) / Revenue. Excludes packaging, shipping subsidy, ads, and fixed overhead.</p>
                <canvas id="productGrossMarginChart"></canvas>
            </div>
            <div class="chart-container">
                <h2 class="chart-title">Daily Facebook Ads</h2>
                <p class="chart-explanation">Facebook advertising spend per day</p>
                <canvas id="fbAdsChart"></canvas>
            </div>
        </div>
        
        <div class="chart-grid">
            <div class="chart-container">
                <h2 class="chart-title">Daily Google Ads</h2>
                <p class="chart-explanation">Google advertising spend per day</p>
                <canvas id="googleAdsChart"></canvas>
            </div>
            <div class="chart-container">
                <h2 class="chart-title">Ads Comparison (FB vs Google)</h2>
                <p class="chart-explanation">Side-by-side comparison of Facebook and Google advertising spend per day</p>
                <canvas id="adsComparisonChart"></canvas>
            </div>
        </div>
        
        <div class="chart-grid">
            <div class="chart-container">
                <h2 class="chart-title">Daily Packaging Costs</h2>
                <p class="chart-explanation">Cost of packaging materials per order (calculated using configured per-order packaging cost)</p>
                <canvas id="packagingCostsChart"></canvas>
            </div>
            <div class="chart-container">
                <h2 class="chart-title">Daily Shipping Subsidy</h2>
                <p class="chart-explanation">Postal subsidy paid per order (configured as fixed amount per order)</p>
                <canvas id="shippingSubsidyChart"></canvas>
            </div>
            <div class="chart-container">
                <h2 class="chart-title">Daily Fixed Costs</h2>
                <p class="chart-explanation">Fixed daily operational costs (overhead, utilities, etc.) distributed evenly across days</p>
                <canvas id="fixedCostsChart"></canvas>
            </div>
        </div>
        
        <div class="chart-grid">
            <div class="chart-container">
                <h2 class="chart-title">Daily Average Order Value</h2>
                <p class="chart-explanation">AOV (Average Order Value) = Total Revenue / Number of Orders. Measures average spending per order</p>
                <canvas id="aovChart"></canvas>
            </div>
            <div class="chart-container">
                <h2 class="chart-title">Daily Items Sold</h2>
                <p class="chart-explanation">Total number of individual product items sold (not orders - one order can contain multiple items)</p>
                <canvas id="itemsChart"></canvas>
            </div>
        </div>

        <div class="chart-container">
            <h2 class="chart-title">Average Items per Order</h2>
            <p class="chart-explanation">Average number of items per order = Total Items / Number of Orders. Indicates basket size</p>
            <canvas id="avgItemsPerOrderChart"></canvas>
        </div>

        <div class="chart-container">
            <h2 class="chart-title">Daily Contribution per Order (Pre-Ad vs Post-Ad)</h2>
            <p class="chart-explanation">Pre-Ad contribution/order = (Revenue - Product Costs - Packaging - Shipping Subsidy) / Orders. Post-Ad contribution/order additionally subtracts Ads. Fixed overhead excluded in both.</p>
            <canvas id="contributionPerOrderChart"></canvas>
        </div>

        <div class="chart-container">
            <h2 class="chart-title">Average Daily Revenue and Profit Trend</h2>
            <p class="chart-explanation">Cumulative daily averages in time: average revenue/day and average profit/loss per day from the start of selected period</p>
            <canvas id="avgDailyTrendChart"></canvas>
        </div>"""

    if new_vs_returning_revenue and new_vs_returning_revenue.get('daily') is not None and not new_vs_returning_revenue.get('daily').empty:
        html_content += """

        <div class="chart-grid">
            <div class="chart-container">
                <h2 class="chart-title">New vs Returning Revenue Split</h2>
                <p class="chart-explanation">Share of net revenue generated by first-time vs returning customer orders in selected period.</p>
                <canvas id="newReturningRevenuePieChart"></canvas>
            </div>
            <div class="chart-container">
                <h2 class="chart-title">New vs Returning Revenue Trend</h2>
                <p class="chart-explanation">Daily net revenue trend split by new-customer vs returning-customer orders.</p>
                <canvas id="newReturningRevenueTrendChart"></canvas>
            </div>
        </div>"""

    if refunds_analysis and refunds_analysis.get('daily') is not None and not refunds_analysis.get('daily').empty:
        html_content += """

        <div class="chart-grid">
            <div class="chart-container">
                <h2 class="chart-title">Refund Rate Trend</h2>
                <p class="chart-explanation">Daily share of refunded/returned orders based on refund-related order statuses.</p>
                <canvas id="refundRateChart"></canvas>
            </div>
            <div class="chart-container">
                <h2 class="chart-title">Refund Amount Trend</h2>
                <p class="chart-explanation">Daily refunded amount based on orders marked as returned/refunded in order status.</p>
                <canvas id="refundAmountChart"></canvas>
            </div>
        </div>"""

    # Add order size distribution chart if data is available
    if order_size_distribution is not None and not order_size_distribution.empty:
        html_content += """

        <div class="chart-container">
            <h2 class="chart-title">Order Size Distribution (Items per Order)</h2>
            <p class="chart-explanation">Breakdown of orders by number of items purchased: shows how many orders contain 1, 2, 3, 4, or 5+ items</p>
            <canvas id="orderSizeDistributionChart"></canvas>
        </div>"""

    # Add Facebook Ads Analytics section if data is available
    if fb_detailed_metrics or fb_campaigns:
        html_content += """

        <h2 style="text-align: center; color: white; margin: 40px 0 20px; font-size: 2rem;">Facebook Ads Analytics</h2>"""

        # Add detailed metrics charts
        if fb_detailed_metrics:
            # Prepare data for charts
            fb_dates = sorted(fb_detailed_metrics.keys())
            fb_impressions = [fb_detailed_metrics[d].get('impressions', 0) for d in fb_dates]
            fb_clicks = [fb_detailed_metrics[d].get('clicks', 0) for d in fb_dates]
            fb_reach = [fb_detailed_metrics[d].get('reach', 0) for d in fb_dates]
            fb_ctr = [fb_detailed_metrics[d].get('ctr', 0) for d in fb_dates]
            fb_cpc = [fb_detailed_metrics[d].get('cpc', 0) for d in fb_dates]
            fb_cpm = [fb_detailed_metrics[d].get('cpm', 0) for d in fb_dates]
            fb_spend_detailed = [fb_detailed_metrics[d].get('spend', 0) for d in fb_dates]

            # Calculate totals
            total_impressions = sum(fb_impressions)
            total_clicks = sum(fb_clicks)
            total_reach = sum(fb_reach)
            total_fb_spend_detailed = sum(fb_spend_detailed)
            avg_ctr = total_clicks / total_impressions * 100 if total_impressions > 0 else 0
            avg_cpc = total_fb_spend_detailed / total_clicks if total_clicks > 0 else 0
            avg_cpm = total_fb_spend_detailed / total_impressions * 1000 if total_impressions > 0 else 0

            html_content += f"""

        <div class="summary-cards">
            <div class="card">
                <div class="card-title">Total Impressions</div>
                <div class="card-value">{total_impressions:,}</div>
            </div>
            <div class="card">
                <div class="card-title">Total Clicks</div>
                <div class="card-value">{total_clicks:,}</div>
            </div>
            <div class="card">
                <div class="card-title">Total Reach</div>
                <div class="card-value">{total_reach:,}</div>
            </div>
            <div class="card">
                <div class="card-title">Avg CTR</div>
                <div class="card-value">{avg_ctr:.2f}%</div>
            </div>
            <div class="card">
                <div class="card-title">Avg CPC</div>
                <div class="card-value cost">€{avg_cpc:.2f}</div>
            </div>
            <div class="card">
                <div class="card-title">Avg CPM</div>
                <div class="card-value cost">€{avg_cpm:.2f}</div>
            </div>
        </div>

        <div class="chart-grid">
            <div class="chart-container">
                <h2 class="chart-title">Daily Impressions & Reach</h2>
                <p class="chart-explanation">Impressions = total ad views | Reach = unique users who saw the ad. High frequency = impressions/reach ratio</p>
                <canvas id="fbImpressionsReachChart"></canvas>
            </div>
            <div class="chart-container">
                <h2 class="chart-title">Daily Clicks</h2>
                <p class="chart-explanation">Number of clicks on ads per day. Direct indicator of ad engagement</p>
                <canvas id="fbClicksChart"></canvas>
            </div>
        </div>

        <div class="chart-grid">
            <div class="chart-container">
                <h2 class="chart-title">Daily CTR (Click-Through Rate)</h2>
                <p class="chart-explanation">CTR = Clicks / Impressions × 100. Measures how compelling your ads are. Higher CTR = better ad creative/targeting</p>
                <canvas id="fbCtrChart"></canvas>
            </div>
            <div class="chart-container">
                <h2 class="chart-title">Daily CPC (Cost Per Click)</h2>
                <p class="chart-explanation">CPC = Spend / Clicks. Lower is better. Affected by competition, targeting, and ad quality</p>
                <canvas id="fbCpcChart"></canvas>
            </div>
        </div>

        <div class="chart-grid">
            <div class="chart-container">
                <h2 class="chart-title">Daily CPM (Cost Per 1000 Impressions)</h2>
                <p class="chart-explanation">CPM = Spend / Impressions × 1000. Measures cost efficiency of reaching your audience</p>
                <canvas id="fbCpmChart"></canvas>
            </div>
            <div class="chart-container">
                <h2 class="chart-title">Spend vs Clicks Correlation</h2>
                <p class="chart-explanation">Relationship between daily ad spend and clicks generated. Shows cost efficiency over time</p>
                <canvas id="fbSpendClicksChart"></canvas>
            </div>
        </div>

        <div class="chart-container">
            <h2 class="chart-title">FB Ads Efficiency Trends</h2>
            <p class="chart-explanation">Combined view of CPC, CPM, and CTR trends over time. Monitor efficiency changes</p>
            <canvas id="fbEfficiencyTrendsChart"></canvas>
        </div>"""

        # Add campaign performance table
        if fb_campaigns:
            # Filter to campaigns with spend
            active_campaigns = [c for c in fb_campaigns if c.get('spend', 0) > 0]

            if active_campaigns:
                total_campaign_spend = sum(c.get('spend', 0) for c in active_campaigns)
                total_campaign_impressions = sum(c.get('impressions', 0) for c in active_campaigns)
                total_campaign_clicks = sum(c.get('clicks', 0) for c in active_campaigns)

                html_content += f"""

        <div class="table-container">
            <div class="collapsible-header" onclick="toggleCollapse(this)">
                <h2 class="table-title">Campaign Performance</h2>
                <span class="toggle-icon">▼</span>
            </div>
            <div class="collapsible-content">
                <p style="color: #718096; margin-bottom: 15px;">Performance breakdown by campaign. Click headers to sort. Focus on campaigns with best conversion rates and lowest cost per conversion.</p>
                <table>
                    <thead>
                        <tr>
                            <th>Campaign Name</th>
                            <th>Status</th>
                            <th>Objective</th>
                            <th class="number">Spend</th>
                            <th class="number">Impressions</th>
                            <th class="number">Clicks</th>
                            <th class="number">CTR</th>
                            <th class="number">CPC</th>
                            <th class="number">CPM</th>
                            <th class="number">Conversions</th>
                            <th class="number">Conv. Rate</th>
                            <th class="number">Cost/Conv</th>
                            <th class="number">% of Spend</th>
                        </tr>
                    </thead>
                    <tbody>"""

                total_conversions = sum(c.get('conversions', 0) for c in active_campaigns)

                for campaign in active_campaigns:
                    spend = campaign.get('spend', 0)
                    impressions = campaign.get('impressions', 0)
                    clicks = campaign.get('clicks', 0)
                    reach = campaign.get('reach', 0)
                    ctr = campaign.get('ctr', 0)
                    cpc = campaign.get('cpc', 0)
                    cpm = campaign.get('cpm', 0)
                    frequency = campaign.get('frequency', 0)
                    conversions = campaign.get('conversions', 0)
                    conversion_rate = campaign.get('conversion_rate', 0)
                    cost_per_conversion = campaign.get('cost_per_conversion', 0)
                    spend_pct = (spend / total_campaign_spend * 100) if total_campaign_spend > 0 else 0
                    status = campaign.get('status', 'UNKNOWN')
                    objective = campaign.get('objective', 'UNKNOWN').replace('_', ' ').title()

                    # Color code status
                    status_color = '#48bb78' if status == 'ACTIVE' else '#718096'

                    # Color code conversion rate (green if above 1%, yellow if 0.5-1%, red if below 0.5%)
                    conv_rate_color = '#48bb78' if conversion_rate > 1 else ('#ecc94b' if conversion_rate > 0.5 else '#f56565') if conversions > 0 else '#718096'

                    html_content += f"""
                        <tr>
                            <td>{campaign.get('campaign_name', 'Unknown')}</td>
                            <td style="color: {status_color};">{status}</td>
                            <td>{objective}</td>
                            <td class="number">€{spend:.2f}</td>
                            <td class="number">{impressions:,}</td>
                            <td class="number">{clicks:,}</td>
                            <td class="number">{ctr:.2f}%</td>
                            <td class="number">€{cpc:.2f}</td>
                            <td class="number">€{cpm:.2f}</td>
                            <td class="number">{conversions}</td>
                            <td class="number" style="color: {conv_rate_color}; font-weight: bold;">{conversion_rate:.2f}%</td>
                            <td class="number">{'€' + f'{cost_per_conversion:.2f}' if cost_per_conversion > 0 else '-'}</td>
                            <td class="number">{spend_pct:.1f}%</td>
                        </tr>"""

                avg_conversion_rate = (total_conversions / total_campaign_clicks * 100) if total_campaign_clicks > 0 else 0
                avg_cost_per_conversion = (total_campaign_spend / total_conversions) if total_conversions > 0 else 0

                html_content += f"""
                        <tr class="total-row">
                            <td><strong>TOTAL / AVG</strong></td>
                            <td></td>
                            <td></td>
                            <td class="number"><strong>€{total_campaign_spend:.2f}</strong></td>
                            <td class="number"><strong>{total_campaign_impressions:,}</strong></td>
                            <td class="number"><strong>{total_campaign_clicks:,}</strong></td>
                            <td class="number"><strong>{(total_campaign_clicks / total_campaign_impressions * 100) if total_campaign_impressions > 0 else 0:.2f}%</strong></td>
                            <td class="number"><strong>€{(total_campaign_spend / total_campaign_clicks) if total_campaign_clicks > 0 else 0:.2f}</strong></td>
                            <td class="number"><strong>€{(total_campaign_spend / total_campaign_impressions * 1000) if total_campaign_impressions > 0 else 0:.2f}</strong></td>
                            <td class="number"><strong>{total_conversions}</strong></td>
                            <td class="number"><strong>{avg_conversion_rate:.2f}%</strong></td>
                            <td class="number"><strong>{'€' + f'{avg_cost_per_conversion:.2f}' if avg_cost_per_conversion > 0 else '-'}</strong></td>
                            <td class="number"><strong>100%</strong></td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </div>

        <div class="chart-container">
            <h2 class="chart-title">Campaign Spend Distribution</h2>
            <p class="chart-explanation">Breakdown of ad spend by campaign. Larger segments indicate higher investment</p>
            <canvas id="campaignSpendPieChart"></canvas>
        </div>

        <div class="chart-container">
            <h2 class="chart-title">Campaign Efficiency Comparison</h2>
            <p class="chart-explanation">Compare campaigns by CPC (cost per click) - lower bars indicate more efficient campaigns</p>
            <canvas id="campaignCpcComparisonChart"></canvas>
        </div>

        <div class="chart-container">
            <h2 class="chart-title">Campaign CTR Comparison</h2>
            <p class="chart-explanation">Click-through rate comparison across campaigns - higher bars indicate better engagement</p>
            <canvas id="campaignCtrComparisonChart"></canvas>
        </div>

        <div class="chart-container">
            <h2 class="chart-title">Campaign Conversion Rate Comparison</h2>
            <p class="chart-explanation">Conversion rate comparison across campaigns - higher bars indicate better performance</p>
            <canvas id="campaignConversionRateChart"></canvas>
        </div>

        <div class="chart-container">
            <h2 class="chart-title">Campaign Cost Per Conversion Comparison</h2>
            <p class="chart-explanation">Cost per conversion comparison - lower bars indicate more efficient conversion spending</p>
            <canvas id="campaignCostPerConversionChart"></canvas>
        </div>"""

    # Add Cost Per Order Analysis section
    if cost_per_order:
        fb_cpo = cost_per_order.get('fb_cpo', 0)
        total_orders_cpo = cost_per_order.get('total_orders', 0)
        total_fb_spend_cpo = cost_per_order.get('total_fb_spend', 0)
        total_revenue_cpo = cost_per_order.get('total_revenue', 0)
        total_revenue_raw_cpo = cost_per_order.get('total_revenue_raw', total_revenue_cpo)
        total_revenue_source_cpo = cost_per_order.get('total_revenue_source', 'daily_data.revenue')
        fb_recon = cost_per_order.get('fb_spend_reconciliation', {}) or {}
        fb_spend_daily_source = fb_recon.get('daily_source_spend')
        fb_spend_campaign_source = fb_recon.get('campaign_source_spend')
        fb_spend_diff = fb_recon.get('difference')
        fb_spend_diff_pct = fb_recon.get('difference_pct')
        time_lagged = cost_per_order.get('time_lagged_analysis', {})
        best_lag = cost_per_order.get('best_attribution_lag', '0_day')
        best_lag_corr = cost_per_order.get('best_lag_correlation', 0)
        campaign_attribution = cost_per_order.get('campaign_attribution', [])
        weekly_cpo = cost_per_order.get('weekly_cpo', [])
        best_cpo_days = cost_per_order.get('best_cpo_days', [])
        worst_cpo_days = cost_per_order.get('worst_cpo_days', [])

        # Calculate ROAS
        overall_roas = total_revenue_cpo / total_fb_spend_cpo if total_fb_spend_cpo > 0 else 0

        html_content += f"""

        <h2 style="text-align: center; color: white; margin: 40px 0 20px; font-size: 2rem;">💰 Cost Per Order Analysis</h2>
        <p style="text-align: center; color: rgba(255,255,255,0.8); margin-bottom: 20px;">Estimated order attribution based on click and spend distribution. Note: This is correlation-based estimation, not direct tracking.</p>

        <div class="summary-cards">
            <div class="card">
                <div class="card-title">FB Cost Per Order</div>
                <div class="card-value cost">€{fb_cpo:.2f}</div>
            </div>
            <div class="card">
                <div class="card-title">Total FB Spend</div>
                <div class="card-value cost">€{total_fb_spend_cpo:,.2f}</div>
            </div>
            <div class="card">
                <div class="card-title">Total Orders</div>
                <div class="card-value">{total_orders_cpo:,}</div>
            </div>
            <div class="card">
                <div class="card-title">Total Revenue (Net)</div>
                <div class="card-value profit">€{total_revenue_cpo:,.2f}</div>
            </div>
            <div class="card">
                <div class="card-title">Overall ROAS (FB)</div>
                <div class="card-value {'profit' if overall_roas > 1 else 'cost'}">{overall_roas:.2f}x</div>
            </div>
            <div class="card">
                <div class="card-title">Best Attribution Lag</div>
                <div class="card-value roi">{best_lag.replace('_', ' ').title()}</div>
            </div>
        </div>"""

        # FB spend and revenue reconciliation (daily source vs campaign source)
        if fb_spend_daily_source is not None:
            diff_class = 'profit-positive' if (fb_spend_diff is not None and abs(fb_spend_diff) <= 0.01) else 'profit-negative'
            campaign_spend_display = f"€{fb_spend_campaign_source:,.2f}" if fb_spend_campaign_source is not None else "N/A"
            diff_display = f"€{fb_spend_diff:+,.2f}" if fb_spend_diff is not None else "N/A"
            diff_pct_display = f"{fb_spend_diff_pct:+.2f}%" if fb_spend_diff_pct is not None else "N/A"
            html_content += f"""

        <div class="table-container">
            <div class="collapsible-header expanded" onclick="toggleCollapse(this)">
                <h2 class="table-title">Spend/Revenue Reconciliation</h2>
                <span class="toggle-icon">▼</span>
            </div>
            <div class="collapsible-content expanded">
                <table>
                    <thead>
                        <tr>
                            <th>Metric</th>
                            <th>Value</th>
                            <th>Note</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td>FB Spend (Daily Source)</td>
                            <td class="number">€{fb_spend_daily_source:,.2f}</td>
                            <td>Sum of daily FB spend mapped to report days</td>
                        </tr>
                        <tr>
                            <td>FB Spend (Campaign Source)</td>
                            <td class="number">{campaign_spend_display}</td>
                            <td>Sum of campaign-level FB spend</td>
                        </tr>
                        <tr>
                            <td>FB Spend Difference</td>
                            <td class="number {diff_class}">{diff_display} ({diff_pct_display})</td>
                            <td>Daily source - campaign source</td>
                        </tr>
                        <tr>
                            <td>Revenue Used in CPO Section</td>
                            <td class="number">€{total_revenue_cpo:,.2f}</td>
                            <td>Source: {total_revenue_source_cpo}</td>
                        </tr>
                        <tr>
                            <td>Raw Revenue from Daily CPO Aggregation</td>
                            <td class="number">€{total_revenue_raw_cpo:,.2f}</td>
                            <td>For comparison only</td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </div>"""

        # Time-lagged correlation analysis
        if time_lagged:
            html_content += f"""

        <div class="chart-container">
            <h2 class="chart-title">Time-Lagged Attribution Analysis</h2>
            <p class="chart-explanation">Correlation between FB spend and orders at different time lags. Higher correlation at lag N suggests orders come N days after ad exposure. This helps understand the customer journey length.</p>
            <div style="display: flex; justify-content: space-around; flex-wrap: wrap; padding: 20px;">"""

            for lag, corr in time_lagged.items():
                lag_label = lag.replace('_', ' ').title()
                color = '#48bb78' if corr > 0.3 else '#ed8936' if corr > 0 else '#f56565'
                bar_width = max(5, abs(corr) * 100)
                html_content += f"""
                <div style="text-align: center; margin: 10px 20px;">
                    <div style="font-size: 1.5rem; font-weight: bold; color: {color};">{corr:.3f}</div>
                    <div style="background: {color}; height: 10px; width: {bar_width}px; margin: 10px auto; border-radius: 5px;"></div>
                    <div style="color: #718096; font-size: 0.9rem;">{lag_label}</div>
                </div>"""

            html_content += f"""
            </div>
            <p style="text-align: center; color: #718096; margin-top: 10px;">Best correlation at <strong>{best_lag.replace('_', ' ')}</strong> ({best_lag_corr:.3f}) - suggests orders typically come {best_lag.split('_')[0]} day(s) after seeing ads</p>
        </div>"""

        # Campaign Attribution Table
        if campaign_attribution:
            html_content += f"""

        <div class="table-container">
            <div class="collapsible-header expanded" onclick="toggleCollapse(this)">
                <h2 class="table-title">Estimated Campaign Attribution</h2>
                <span class="toggle-icon">▼</span>
            </div>
            <div class="collapsible-content expanded">
                <p style="color: #718096; margin-bottom: 15px;">Estimated orders attributed to each campaign based on click and spend distribution (60% click-weighted, 40% spend-weighted). Best performers are listed first.</p>
                <table>
                    <thead>
                        <tr>
                            <th>Campaign</th>
                            <th class="number">Spend</th>
                            <th class="number">Clicks</th>
                            <th class="number">CTR</th>
                            <th class="number">CPC</th>
                            <th class="number">Est. Orders</th>
                            <th class="number">Est. CPO</th>
                            <th class="number">Est. Revenue</th>
                            <th class="number">Est. ROAS</th>
                            <th class="number">Click Share</th>
                        </tr>
                    </thead>
                    <tbody>"""

            for camp in campaign_attribution:
                cpo_color = '#48bb78' if camp['estimated_cpo'] < fb_cpo else '#f56565'
                roas_color = '#48bb78' if camp['estimated_roas'] > 1 else '#f56565'

                html_content += f"""
                        <tr>
                            <td>{camp['campaign_name'][:40]}</td>
                            <td class="number">€{camp['spend']:.2f}</td>
                            <td class="number">{camp['clicks']:,}</td>
                            <td class="number">{camp['ctr']:.2f}%</td>
                            <td class="number">€{camp['cpc']:.2f}</td>
                            <td class="number">{camp['estimated_orders']:.1f}</td>
                            <td class="number" style="color: {cpo_color}; font-weight: bold;">€{camp['estimated_cpo']:.2f}</td>
                            <td class="number">€{camp['estimated_revenue']:,.2f}</td>
                            <td class="number" style="color: {roas_color}; font-weight: bold;">{camp['estimated_roas']:.2f}x</td>
                            <td class="number">{camp['click_share_pct']:.1f}%</td>
                        </tr>"""

            html_content += """
                    </tbody>
                </table>
            </div>
        </div>"""

        # Weekly CPO Trend Chart
        if weekly_cpo:
            html_content += """

        <div class="chart-container">
            <h2 class="chart-title">Weekly Cost Per Order Trend</h2>
            <p class="chart-explanation">How efficiently ad spend converts to orders over time. Lower CPO = more efficient acquisition</p>
            <canvas id="weeklyCpoChart"></canvas>
        </div>"""

        # Best and Worst CPO Days
        if best_cpo_days or worst_cpo_days:
            html_content += """

        <div class="chart-grid">"""

            if best_cpo_days:
                html_content += """
            <div class="chart-container">
                <h2 class="chart-title" style="color: #48bb78;">Best CPO Days</h2>
                <p class="chart-explanation">Days with lowest cost per order - most efficient ad spend</p>
                <table>
                    <thead>
                        <tr><th>Date</th><th class="number">Orders</th><th class="number">FB Spend</th><th class="number">CPO</th><th class="number">ROAS</th></tr>
                    </thead>
                    <tbody>"""
                for day in best_cpo_days:
                    html_content += f"""
                        <tr>
                            <td>{day['date']}</td>
                            <td class="number">{day['orders']}</td>
                            <td class="number">€{day['fb_spend']:.2f}</td>
                            <td class="number profit-positive">€{day['cpo']:.2f}</td>
                            <td class="number">{day['roas']:.2f}x</td>
                        </tr>"""
                html_content += """
                    </tbody>
                </table>
            </div>"""

            if worst_cpo_days:
                html_content += """
            <div class="chart-container">
                <h2 class="chart-title" style="color: #f56565;">Worst CPO Days</h2>
                <p class="chart-explanation">Days with highest cost per order - least efficient ad spend</p>
                <table>
                    <thead>
                        <tr><th>Date</th><th class="number">Orders</th><th class="number">FB Spend</th><th class="number">CPO</th><th class="number">ROAS</th></tr>
                    </thead>
                    <tbody>"""
                for day in worst_cpo_days:
                    html_content += f"""
                        <tr>
                            <td>{day['date']}</td>
                            <td class="number">{day['orders']}</td>
                            <td class="number">€{day['fb_spend']:.2f}</td>
                            <td class="number profit-negative">€{day['cpo']:.2f}</td>
                            <td class="number">{day['roas']:.2f}x</td>
                        </tr>"""
                html_content += """
                    </tbody>
                </table>
            </div>"""

            html_content += """
        </div>"""

        # Campaign CPO Comparison Chart
        if campaign_attribution:
            html_content += """

        <div class="chart-container">
            <h2 class="chart-title">Campaign Estimated CPO Comparison</h2>
            <p class="chart-explanation">Estimated Cost Per Order by campaign - lower is better. Green = below average, Red = above average</p>
            <canvas id="campaignCpoChart"></canvas>
        </div>

        <div class="chart-container">
            <h2 class="chart-title">Campaign Estimated ROAS Comparison</h2>
            <p class="chart-explanation">Estimated Return on Ad Spend by campaign - higher is better. Green = profitable (>1x), Red = unprofitable (<1x)</p>
            <canvas id="campaignRoasChart"></canvas>
        </div>"""

    # Add Time of Day Analysis section
    if fb_hourly_stats or fb_dow_stats:
        html_content += """

        <h2 style="text-align: center; color: white; margin: 40px 0 20px; font-size: 2rem;">⏰ FB Ads Time-Based Analysis</h2>
        <p style="text-align: center; color: rgba(255,255,255,0.8); margin-bottom: 20px;">Analyze when your Facebook ads perform best - by hour of day and day of week</p>"""

        # Hourly Stats Section
        if fb_hourly_stats:
            # Find best and worst hours
            if fb_hourly_stats:
                best_ctr_hour = max(fb_hourly_stats, key=lambda x: x.get('ctr', 0))
                best_cpc_hour = min([h for h in fb_hourly_stats if h.get('cpc', 0) > 0], key=lambda x: x.get('cpc', float('inf')), default=None)
                best_clicks_hour = max(fb_hourly_stats, key=lambda x: x.get('clicks', 0))

                total_hourly_spend = sum(h.get('spend', 0) for h in fb_hourly_stats)
                total_hourly_clicks = sum(h.get('clicks', 0) for h in fb_hourly_stats)
                total_hourly_impressions = sum(h.get('impressions', 0) for h in fb_hourly_stats)

                html_content += f"""

        <div class="summary-cards">
            <div class="card">
                <div class="card-title">Best CTR Hour</div>
                <div class="card-value profit">{best_ctr_hour['hour']:02d}:00</div>
                <div style="color: #718096; font-size: 0.8rem;">CTR: {best_ctr_hour.get('ctr', 0):.2f}%</div>
            </div>"""

                if best_cpc_hour:
                    html_content += f"""
            <div class="card">
                <div class="card-title">Best CPC Hour</div>
                <div class="card-value profit">{best_cpc_hour['hour']:02d}:00</div>
                <div style="color: #718096; font-size: 0.8rem;">CPC: €{best_cpc_hour.get('cpc', 0):.2f}</div>
            </div>"""

                html_content += f"""
            <div class="card">
                <div class="card-title">Most Clicks Hour</div>
                <div class="card-value">{best_clicks_hour['hour']:02d}:00</div>
                <div style="color: #718096; font-size: 0.8rem;">{best_clicks_hour.get('clicks', 0):,} clicks</div>
            </div>
            <div class="card">
                <div class="card-title">Total Hourly Spend</div>
                <div class="card-value cost">€{total_hourly_spend:,.2f}</div>
            </div>
        </div>

        <div class="chart-grid">
            <div class="chart-container">
                <h2 class="chart-title">Hourly CTR (Click-Through Rate)</h2>
                <p class="chart-explanation">CTR by hour of day - higher is better. Shows when your ads are most compelling</p>
                <canvas id="hourlyCtrChart"></canvas>
            </div>
            <div class="chart-container">
                <h2 class="chart-title">Hourly CPC (Cost Per Click)</h2>
                <p class="chart-explanation">CPC by hour of day - lower is better. Shows when clicks are cheapest</p>
                <canvas id="hourlyCpcChart"></canvas>
            </div>
        </div>

        <div class="chart-grid">
            <div class="chart-container">
                <h2 class="chart-title">Hourly Clicks Distribution</h2>
                <p class="chart-explanation">Number of clicks by hour - shows when your audience is most active</p>
                <canvas id="hourlyClicksChart"></canvas>
            </div>
            <div class="chart-container">
                <h2 class="chart-title">Hourly Spend Distribution</h2>
                <p class="chart-explanation">Ad spend by hour - shows budget distribution throughout the day</p>
                <canvas id="hourlySpendChart"></canvas>
            </div>
        </div>

        <div class="chart-container">
            <h2 class="chart-title">Hourly Efficiency Overview</h2>
            <p class="chart-explanation">Combined view: Spend (bars) vs CTR (line) by hour. Find hours with high CTR and reasonable spend</p>
            <canvas id="hourlyEfficiencyChart"></canvas>
        </div>"""

        # Add Hourly CPO section if we have both hourly FB stats and hourly orders
        hourly_orders = cost_per_order.get('hourly_orders', []) if cost_per_order else []
        if fb_hourly_stats and hourly_orders:
            # Create a dict for quick lookup of orders by hour
            orders_by_hour = {h['hour']: h for h in hourly_orders}

            # Calculate hourly CPO by matching spend and orders
            hourly_cpo_data = []
            for fb_hour in fb_hourly_stats:
                hour = fb_hour['hour']
                spend = fb_hour.get('spend', 0)
                order_data = orders_by_hour.get(hour, {'orders': 0, 'revenue': 0})
                orders = order_data.get('orders', 0)
                revenue = order_data.get('revenue', 0)

                cpo = spend / orders if orders > 0 else 0
                roas = revenue / spend if spend > 0 else 0

                hourly_cpo_data.append({
                    'hour': hour,
                    'spend': spend,
                    'orders': orders,
                    'revenue': revenue,
                    'cpo': cpo,
                    'roas': roas
                })

            # Find best and worst CPO hours
            valid_cpo_hours = [h for h in hourly_cpo_data if h['cpo'] > 0]
            if valid_cpo_hours:
                best_cpo_hour = min(valid_cpo_hours, key=lambda x: x['cpo'])
                worst_cpo_hour = max(valid_cpo_hours, key=lambda x: x['cpo'])
                avg_hourly_cpo = sum(h['cpo'] for h in valid_cpo_hours) / len(valid_cpo_hours)

                # Best ROAS hour
                valid_roas_hours = [h for h in hourly_cpo_data if h['roas'] > 0]
                best_roas_hour = max(valid_roas_hours, key=lambda x: x['roas']) if valid_roas_hours else None

                html_content += f"""

        <h3 style="text-align: center; color: white; margin: 30px 0 15px; font-size: 1.5rem;">Hourly Cost Per Order Analysis</h3>

        <div class="summary-cards">
            <div class="card">
                <div class="card-title">Best CPO Hour</div>
                <div class="card-value profit">{best_cpo_hour['hour']:02d}:00</div>
                <div style="color: #718096; font-size: 0.8rem;">CPO: €{best_cpo_hour['cpo']:.2f}</div>
            </div>
            <div class="card">
                <div class="card-title">Worst CPO Hour</div>
                <div class="card-value cost">{worst_cpo_hour['hour']:02d}:00</div>
                <div style="color: #718096; font-size: 0.8rem;">CPO: €{worst_cpo_hour['cpo']:.2f}</div>
            </div>"""

                if best_roas_hour:
                    html_content += f"""
            <div class="card">
                <div class="card-title">Best ROAS Hour</div>
                <div class="card-value profit">{best_roas_hour['hour']:02d}:00</div>
                <div style="color: #718096; font-size: 0.8rem;">ROAS: {best_roas_hour['roas']:.2f}x</div>
            </div>"""

                html_content += f"""
            <div class="card">
                <div class="card-title">Avg Hourly CPO</div>
                <div class="card-value">€{avg_hourly_cpo:.2f}</div>
            </div>
        </div>

        <div class="chart-grid">
            <div class="chart-container">
                <h2 class="chart-title">Hourly Cost Per Order</h2>
                <p class="chart-explanation">CPO by hour - lower is better. Green = below average, Red = above average. Shows when ad spend converts most efficiently</p>
                <canvas id="hourlyCpoChart"></canvas>
            </div>
            <div class="chart-container">
                <h2 class="chart-title">Hourly Orders Distribution</h2>
                <p class="chart-explanation">Number of orders by hour of day - shows when customers are buying</p>
                <canvas id="hourlyOrdersChart"></canvas>
            </div>
        </div>

        <div class="chart-container">
            <h2 class="chart-title">Hourly ROAS (Return on Ad Spend)</h2>
            <p class="chart-explanation">Revenue / Spend by hour. Higher = more profitable. Green = profitable (>1x), Red = unprofitable</p>
            <canvas id="hourlyRoasChart"></canvas>
        </div>

        <div class="chart-container">
            <h2 class="chart-title">Hourly Spend vs Orders vs CPO</h2>
            <p class="chart-explanation">Combined view: FB Spend (bars), Orders (blue line), CPO (red line). Find hours with high orders and low CPO</p>
            <canvas id="hourlySpendOrdersCpoChart"></canvas>
        </div>"""

                # Hourly stats table
                html_content += """

        <div class="table-container">
            <div class="collapsible-header" onclick="toggleCollapse(this)">
                <h2 class="table-title">Hourly Performance Details</h2>
                <span class="toggle-icon">▼</span>
            </div>
            <div class="collapsible-content">
                <table>
                    <thead>
                        <tr>
                            <th>Hour</th>
                            <th class="number">Spend</th>
                            <th class="number">Impressions</th>
                            <th class="number">Clicks</th>
                            <th class="number">CTR</th>
                            <th class="number">CPC</th>
                            <th class="number">CPM</th>
                            <th class="number">Reach</th>
                        </tr>
                    </thead>
                    <tbody>"""

                for hour in fb_hourly_stats:
                    html_content += f"""
                        <tr>
                            <td>{hour['hour']:02d}:00 - {hour['hour']:02d}:59</td>
                            <td class="number">€{hour.get('spend', 0):.2f}</td>
                            <td class="number">{hour.get('impressions', 0):,}</td>
                            <td class="number">{hour.get('clicks', 0):,}</td>
                            <td class="number">{hour.get('ctr', 0):.2f}%</td>
                            <td class="number">€{hour.get('cpc', 0):.2f}</td>
                            <td class="number">€{hour.get('cpm', 0):.2f}</td>
                            <td class="number">{hour.get('reach', 0):,}</td>
                        </tr>"""

                html_content += f"""
                        <tr class="total-row">
                            <td><strong>TOTAL</strong></td>
                            <td class="number"><strong>€{total_hourly_spend:.2f}</strong></td>
                            <td class="number"><strong>{total_hourly_impressions:,}</strong></td>
                            <td class="number"><strong>{total_hourly_clicks:,}</strong></td>
                            <td class="number"><strong>{(total_hourly_clicks/total_hourly_impressions*100) if total_hourly_impressions > 0 else 0:.2f}%</strong></td>
                            <td class="number"><strong>€{(total_hourly_spend/total_hourly_clicks) if total_hourly_clicks > 0 else 0:.2f}</strong></td>
                            <td class="number"><strong>€{(total_hourly_spend/total_hourly_impressions*1000) if total_hourly_impressions > 0 else 0:.2f}</strong></td>
                            <td class="number"></td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </div>"""

        # Day of Week Stats Section
        if fb_dow_stats:
            # Sort by day number
            fb_dow_stats_sorted = sorted(fb_dow_stats, key=lambda x: x.get('day_num', 0))

            best_ctr_day = max(fb_dow_stats, key=lambda x: x.get('ctr', 0))
            best_cpc_day = min([d for d in fb_dow_stats if d.get('cpc', 0) > 0], key=lambda x: x.get('cpc', float('inf')), default=None)

            html_content += f"""

        <div class="chart-grid">
            <div class="chart-container">
                <h2 class="chart-title">Day of Week - CTR</h2>
                <p class="chart-explanation">Average CTR by day of week - which days have the most engaged audience?</p>
                <canvas id="dowCtrChart"></canvas>
            </div>
            <div class="chart-container">
                <h2 class="chart-title">Day of Week - CPC</h2>
                <p class="chart-explanation">Average CPC by day of week - which days offer the cheapest clicks?</p>
                <canvas id="dowCpcChart"></canvas>
            </div>
        </div>

        <div class="chart-container">
            <h2 class="chart-title">Day of Week - Spend vs Clicks</h2>
            <p class="chart-explanation">Compare daily ad spend with clicks received - find the most efficient days</p>
            <canvas id="dowSpendClicksChart"></canvas>
        </div>

        <div class="table-container">
            <div class="collapsible-header" onclick="toggleCollapse(this)">
                <h2 class="table-title">Day of Week Performance</h2>
                <span class="toggle-icon">▼</span>
            </div>
            <div class="collapsible-content">
                <table>
                    <thead>
                        <tr>
                            <th>Day</th>
                            <th class="number">Total Spend</th>
                            <th class="number">Avg Spend</th>
                            <th class="number">Total Clicks</th>
                            <th class="number">Avg Clicks</th>
                            <th class="number">CTR</th>
                            <th class="number">CPC</th>
                            <th class="number">CPM</th>
                            <th class="number">Days Count</th>
                        </tr>
                    </thead>
                    <tbody>"""

            for day in fb_dow_stats_sorted:
                ctr_color = '#48bb78' if day.get('ctr', 0) > best_ctr_day.get('ctr', 0) * 0.9 else ''
                cpc_color = '#48bb78' if best_cpc_day and day.get('cpc', 0) < best_cpc_day.get('cpc', 0) * 1.1 else ''

                html_content += f"""
                        <tr>
                            <td><strong>{day.get('day_of_week', '')}</strong></td>
                            <td class="number">€{day.get('total_spend', 0):,.2f}</td>
                            <td class="number">€{day.get('avg_spend', 0):.2f}</td>
                            <td class="number">{day.get('total_clicks', 0):,}</td>
                            <td class="number">{day.get('avg_clicks', 0):.1f}</td>
                            <td class="number" style="color: {ctr_color};">{day.get('ctr', 0):.2f}%</td>
                            <td class="number" style="color: {cpc_color};">€{day.get('cpc', 0):.2f}</td>
                            <td class="number">€{day.get('cpm', 0):.2f}</td>
                            <td class="number">{day.get('days_count', 0)}</td>
                        </tr>"""

            html_content += """
                    </tbody>
                </table>
            </div>
        </div>"""

    # Add returning customers charts and table if data is available
    if returning_customers_analysis is not None and not returning_customers_analysis.empty:
        html_content += f"""
        
        <h2 style="text-align: center; color: white; margin: 40px 0 20px; font-size: 2rem;">Customer Retention Analysis</h2>
        
        <div class="chart-grid">
            <div class="chart-container">
                <h2 class="chart-title">Customer Type Distribution (Weekly %)</h2>
                <p class="chart-explanation">Percentage of orders from New Customers (first-time buyers) vs Returning Customers (repeat buyers). Measured weekly</p>
                <canvas id="returningPctChart"></canvas>
            </div>
            <div class="chart-container">
                <h2 class="chart-title">Order Volume by Customer Type (Weekly)</h2>
                <p class="chart-explanation">Absolute number of orders from new vs returning customers. Stacked bars show total order volume per week</p>
                <canvas id="returningVolumeChart"></canvas>
            </div>
        </div>

        <div class="chart-container">
            <h2 class="chart-title">New vs Returning Customer Orders (Trend)</h2>
            <p class="chart-explanation">Weekly trend of orders from new customers vs returning customers. Compare growth patterns over time.</p>
            <canvas id="newVsReturningTrendChart"></canvas>
        </div>
        
        <div class="table-container">
            <div class="collapsible-header" onclick="toggleCollapse(this)">
                <h2 class="table-title">Weekly Customer Retention Analysis</h2>
                <span class="toggle-icon">▼</span>
            </div>
            <div class="collapsible-content">
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
            </div>
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
        cumulative_cac = clv_return_time_analysis['cumulative_avg_cac'].tolist() if 'cumulative_avg_cac' in clv_return_time_analysis.columns else [0] * len(clv_weeks)
        ltv_cac_ratio_data = clv_return_time_analysis['ltv_cac_ratio'].tolist() if 'ltv_cac_ratio' in clv_return_time_analysis.columns else [0] * len(clv_weeks)
        pre_ad_contribution_per_order = (
            financial_metrics.get('pre_ad_contribution_per_order', 0)
            if financial_metrics else 0
        )
        payback_weekly_labels = (
            financial_metrics.get('payback_weekly_labels', clv_week_starts)
            if financial_metrics else clv_week_starts
        )
        payback_weekly_orders = (
            financial_metrics.get('payback_weekly_orders', [0] * len(payback_weekly_labels))
            if financial_metrics else [0] * len(payback_weekly_labels)
        )
        
        # Calculate overall metrics
        overall_avg_clv = clv_return_time_analysis['avg_clv'].mean()
        final_cumulative_clv = clv_return_time_analysis['cumulative_avg_clv'].iloc[-1] if not clv_return_time_analysis.empty else 0
        overall_avg_return = clv_return_time_analysis['avg_return_time_days'].mean()
        
        html_content += f"""
        
        <h2 style="text-align: center; color: white; margin: 40px 0 20px; font-size: 2rem;">Customer Lifetime Value, CAC & Return Time Analysis</h2>
        
        <div class="chart-grid">
            <div class="chart-container">
                <h2 class="chart-title">Customer Lifetime Value Trend (Weekly)</h2>
            <p class="chart-explanation">CLV (Customer Lifetime Value) = average net revenue (without VAT) per customer over their lifetime. Avg CLV = weekly average, Cumulative = running average across all time</p>
                <canvas id="clvChart"></canvas>
            </div>
            <div class="chart-container">
                <h2 class="chart-title">Customer Acquisition Cost Trend (Weekly)</h2>
                <p class="chart-explanation">CAC (Customer Acquisition Cost) = Facebook Ads Spend / Number of New Customers. Measures cost to acquire each new customer. Cumulative = running average across all time</p>
                <canvas id="cacChart"></canvas>
            </div>
        </div>
        
        <div class="chart-grid">
            <div class="chart-container">
                <h2 class="chart-title">CLV vs CAC Comparison</h2>
                <p class="chart-explanation">Revenue-based comparison of realized customer lifetime value vs customer acquisition cost. CLV should exceed CAC for profitability.</p>
                <canvas id="clvCacComparisonChart"></canvas>
            </div>
            <div class="chart-container">
                <h2 class="chart-title">Revenue LTV/CAC Ratio Trend</h2>
                <p class="chart-explanation">Revenue LTV/CAC = realized CLV ÷ CAC. Healthy ratio is typically 3:1 or higher. Break-even is 1:1.</p>
                <canvas id="ltvCacRatioChart"></canvas>
            </div>
        </div>

        <div class="chart-container">
            <h2 class="chart-title">Estimated Payback Period Trend (Weekly)</h2>
            <p class="chart-explanation">Payback (orders) = Weekly CAC / Pre-ad contribution per order. Values below 1.0 mean CAC is recovered on the first order contribution.</p>
            <canvas id="paybackChart"></canvas>
        </div>
        
        <div class="chart-container">
            <h2 class="chart-title">Average Customer Return Time (Days)</h2>
            <p class="chart-explanation">Average number of days between a customer's first and second purchase. Shows how quickly customers return to buy again</p>
            <canvas id="returnTimeChart"></canvas>
        </div>
        
        <div class="table-container">
            <div class="collapsible-header" onclick="toggleCollapse(this)">
                <h2 class="table-title">Weekly CLV, CAC & Return Time Analysis</h2>
                <span class="toggle-icon">▼</span>
            </div>
            <div class="collapsible-content">
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
                        <th class="number">Revenue Net (€)</th>
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
            </div>
        </div>"""

    # Add Repeat Purchase Cohort Analysis section
    if cohort_analysis is not None:
        summary = cohort_analysis.get('summary', {})
        time_to_nth = cohort_analysis.get('time_to_nth_order', pd.DataFrame())
        time_between = cohort_analysis.get('time_between_orders', pd.DataFrame())
        order_freq = cohort_analysis.get('order_frequency', pd.DataFrame())
        revenue_by_order = cohort_analysis.get('revenue_by_order_num', pd.DataFrame())
        cohort_retention = cohort_analysis.get('cohort_retention', pd.DataFrame())

        html_content += f"""

        <h2 style="text-align: center; color: white; margin: 40px 0 20px; font-size: 2rem;">Repeat Purchase Cohort Analysis</h2>

        <div class="summary-cards" style="grid-template-columns: repeat(5, 1fr);">
            <div class="card">
                <div class="card-title">Repeat Rate</div>
                <div class="card-value roi">{summary.get('repeat_rate_pct', 0):.1f}%</div>
            </div>
            <div class="card">
                <div class="card-title">Avg Days to 2nd Order</div>
                <div class="card-value">{summary.get('avg_days_to_2nd_order', 'N/A') if summary.get('avg_days_to_2nd_order') else 'N/A'}</div>
            </div>
            <div class="card">
                <div class="card-title">Avg Days Between Orders</div>
                <div class="card-value">{summary.get('avg_days_between_orders', 'N/A') if summary.get('avg_days_between_orders') else 'N/A'}</div>
            </div>
            <div class="card">
                <div class="card-title">Repeat Customers</div>
                <div class="card-value">{summary.get('repeat_customers', 0)}</div>
            </div>
            <div class="card">
                <div class="card-title">Max Orders (Customer)</div>
                <div class="card-value">{summary.get('max_orders_by_customer', 0)}</div>
            </div>
        </div>

        <div class="chart-grid">
            <div class="chart-container">
                <h2 class="chart-title">Order Frequency Distribution</h2>
                <p class="chart-explanation">Distribution of customers by how many orders they've placed. Shows one-time vs repeat buyer breakdown</p>
                <canvas id="orderFrequencyChart"></canvas>
            </div>
            <div class="chart-container">
                <h2 class="chart-title">Time Between Repeat Orders</h2>
                <p class="chart-explanation">Distribution of time intervals between consecutive orders for repeat customers</p>
                <canvas id="timeBetweenOrdersChart"></canvas>
            </div>
        </div>

        <div class="chart-container">
            <h2 class="chart-title">Time Between Orders by Order Transition</h2>
            <p class="chart-explanation">Average days between consecutive orders (1st→2nd, 2nd→3rd, etc.). Shows if repeat customers order faster over time</p>
            <canvas id="timeBetweenByOrderChart"></canvas>
        </div>

        <div class="chart-grid">
            <div class="chart-container">
                <h2 class="chart-title">Time to Nth Order (from First Purchase)</h2>
                <p class="chart-explanation">Average days from first purchase to 2nd, 3rd, 4th order etc. Shows how long it takes customers to return</p>
                <canvas id="timeToNthOrderChart"></canvas>
            </div>
            <div class="chart-container">
                <h2 class="chart-title">Order Metrics by Order Sequence</h2>
                <p class="chart-explanation">Compare order size (items), average order value, and average price per item for 1st, 2nd, 3rd order, etc. Shows how buying behavior changes with repeat purchases</p>
                <canvas id="aovByOrderNumChart"></canvas>
            </div>
        </div>"""

        # Add Time to Nth Order table
        if not time_to_nth.empty:
            html_content += """

        <div class="table-container">
            <div class="collapsible-header" onclick="toggleCollapse(this)">
                <h2 class="table-title">Time to Nth Order Analysis</h2>
                <span class="toggle-icon">▼</span>
            </div>
            <div class="collapsible-content">
            <table>
                <thead>
                    <tr>
                        <th>Order</th>
                        <th class="number">Customers</th>
                        <th class="number">Avg Days (from 1st)</th>
                        <th class="number">Median Days (from 1st)</th>
                        <th class="number">Avg Days (from Prev)</th>
                        <th class="number">Median Days (from Prev)</th>
                        <th class="number">Avg Order Value</th>
                    </tr>
                </thead>
                <tbody>"""

            for _, row in time_to_nth.iterrows():
                html_content += f"""
                    <tr>
                        <td>{row['order_number']}</td>
                        <td class="number">{row['customer_count']}</td>
                        <td class="number">{row['avg_days_from_first']}</td>
                        <td class="number">{row['median_days_from_first']}</td>
                        <td class="number">{row['avg_days_from_prev']}</td>
                        <td class="number">{row['median_days_from_prev']}</td>
                        <td class="number">€{row['avg_order_value']:.2f}</td>
                    </tr>"""

            html_content += """
                </tbody>
            </table>
            </div>
        </div>"""

        # Add Order Frequency table
        if not order_freq.empty:
            html_content += """

        <div class="table-container">
            <div class="collapsible-header" onclick="toggleCollapse(this)">
                <h2 class="table-title">Order Frequency Distribution</h2>
                <span class="toggle-icon">▼</span>
            </div>
            <div class="collapsible-content">
            <table>
                <thead>
                    <tr>
                        <th>Frequency</th>
                        <th class="number">Customers</th>
                        <th class="number">% of Customers</th>
                        <th class="number">Total Orders</th>
                        <th class="number">% of Orders</th>
                    </tr>
                </thead>
                <tbody>"""

            for _, row in order_freq.iterrows():
                html_content += f"""
                    <tr>
                        <td>{row['frequency']}</td>
                        <td class="number">{row['customer_count']}</td>
                        <td class="number">{row['customer_pct']}%</td>
                        <td class="number">{row['total_orders']}</td>
                        <td class="number">{row['orders_pct']}%</td>
                    </tr>"""

            # Add totals
            total_customers = order_freq['customer_count'].sum()
            total_orders = order_freq['total_orders'].sum()
            html_content += f"""
                    <tr class="total-row">
                        <td>TOTAL</td>
                        <td class="number">{total_customers}</td>
                        <td class="number">100%</td>
                        <td class="number">{total_orders}</td>
                        <td class="number">100%</td>
                    </tr>
                </tbody>
            </table>
            </div>
        </div>"""

        # Add Cohort Retention table
        if not cohort_retention.empty:
            html_content += """

        <div class="chart-container">
            <h2 class="chart-title">Cohort Retention Rates</h2>
            <p class="chart-explanation">Percentage of customers from each monthly cohort who made 2nd, 3rd, 4th, 5th order. Shows how customer retention varies by acquisition month</p>
            <canvas id="cohortRetentionChart"></canvas>
        </div>

        <div class="table-container">
            <div class="collapsible-header" onclick="toggleCollapse(this)">
                <h2 class="table-title">Monthly Cohort Retention</h2>
                <span class="toggle-icon">▼</span>
            </div>
            <div class="collapsible-content">
            <table>
                <thead>
                    <tr>
                        <th>Cohort</th>
                        <th class="number">Customers</th>
                        <th class="number">2nd Order %</th>
                        <th class="number">3rd Order %</th>
                        <th class="number">4th Order %</th>
                        <th class="number">5th Order %</th>
                        <th class="number">Avg Orders</th>
                        <th class="number">Total Orders</th>
                    </tr>
                </thead>
                <tbody>"""

            for _, row in cohort_retention.iterrows():
                html_content += f"""
                    <tr>
                        <td>{row['cohort']}</td>
                        <td class="number">{row['total_customers']}</td>
                        <td class="number">{row['retention_2nd_pct']}%</td>
                        <td class="number">{row['retention_3rd_pct']}%</td>
                        <td class="number">{row['retention_4th_pct']}%</td>
                        <td class="number">{row['retention_5th_pct']}%</td>
                        <td class="number">{row['avg_orders_per_customer']}</td>
                        <td class="number">{row['total_orders']}</td>
                    </tr>"""

            # Calculate averages for total row
            avg_retention_2nd = cohort_retention['retention_2nd_pct'].mean()
            avg_retention_3rd = cohort_retention['retention_3rd_pct'].mean()
            avg_retention_4th = cohort_retention['retention_4th_pct'].mean()
            avg_retention_5th = cohort_retention['retention_5th_pct'].mean()
            total_cohort_customers = cohort_retention['total_customers'].sum()
            total_cohort_orders = cohort_retention['total_orders'].sum()
            avg_orders = cohort_retention['avg_orders_per_customer'].mean()

            html_content += f"""
                    <tr class="total-row">
                        <td>AVERAGE</td>
                        <td class="number">{total_cohort_customers}</td>
                        <td class="number">{avg_retention_2nd:.1f}%</td>
                        <td class="number">{avg_retention_3rd:.1f}%</td>
                        <td class="number">{avg_retention_4th:.1f}%</td>
                        <td class="number">{avg_retention_5th:.1f}%</td>
                        <td class="number">{avg_orders:.2f}</td>
                        <td class="number">{total_cohort_orders}</td>
                    </tr>
                </tbody>
            </table>
            </div>
        </div>"""

    # Add Time-Bias-Free Cohort Analysis (mature cohorts only)
    if cohort_analysis is not None:
        mature_cohorts = cohort_analysis.get('mature_cohort_retention', pd.DataFrame())
        summary = cohort_analysis.get('summary', {})

        if not mature_cohorts.empty:
            true_retention_2nd = summary.get('true_retention_2nd_pct', 0)
            true_retention_3rd = summary.get('true_retention_3rd_pct', 0)

            html_content += f"""

        <div class="table-container" style="background: linear-gradient(135deg, #10B981 0%, #059669 100%); margin-top: 30px;">
            <h2 class="table-title" style="color: white;">✅ Skutočná retencia (bez časového skreslenia) - Len kohorty 90+ dní</h2>
            <div class="summary-cards" style="grid-template-columns: repeat(4, 1fr); background: rgba(255,255,255,0.95); padding: 20px; border-radius: 8px; margin: 15px;">
                <div class="card" style="background: #ecfdf5;">
                    <div class="card-title">Skutočná 2nd Order Retencia</div>
                    <div class="card-value" style="color: #059669;">{true_retention_2nd}%</div>
                </div>
                <div class="card" style="background: #ecfdf5;">
                    <div class="card-title">Skutočná 3rd Order Retencia</div>
                    <div class="card-value" style="color: #059669;">{true_retention_3rd}%</div>
                </div>
                <div class="card" style="background: #ecfdf5;">
                    <div class="card-title">Počet zrelých kohort</div>
                    <div class="card-value" style="color: #059669;">{len(mature_cohorts)}</div>
                </div>
                <div class="card" style="background: #ecfdf5;">
                    <div class="card-title">Celkom zákazníkov</div>
                    <div class="card-value" style="color: #059669;">{mature_cohorts['total_customers'].sum()}</div>
                </div>
            </div>
            <p style="color: white; padding: 0 15px 15px; font-size: 0.9rem;">
                <strong>Note:</strong> These values are more accurate because they include only cohorts that had enough time (90+ days) for a repeat purchase.
                Nové kohorty (menej ako 90 dní) nie sú zahrnuté, aby nedochádzalo k skresleniu výsledkov.
            </p>
        </div>

        <div class="chart-container">
            <h2 class="chart-title">Skutočná retencia podľa zrelých kohort (90+ dní)</h2>
            <p class="chart-explanation">Retencia len pre kohorty, ktoré mali dostatok času na opätovný nákup. Tieto čísla sú presnejšie ako celkové štatistiky.</p>
            <canvas id="matureCohortRetentionChart"></canvas>
        </div>

        <div class="table-container">
            <div class="collapsible-header" onclick="toggleCollapse(this)">
                <h2 class="table-title">Zrelé kohorty - Podrobná retencia (90+ dní staré)</h2>
                <span class="toggle-icon">▼</span>
            </div>
            <div class="collapsible-content">
            <table>
                <thead>
                    <tr>
                        <th>Kohorta</th>
                        <th class="number">Vek (dní)</th>
                        <th class="number">Zákazníci</th>
                        <th class="number">2nd Order %</th>
                        <th class="number">3rd Order %</th>
                        <th class="number">4th Order %</th>
                        <th class="number">5th Order %</th>
                        <th class="number">Priemer obj.</th>
                    </tr>
                </thead>
                <tbody>"""

            for _, row in mature_cohorts.iterrows():
                html_content += f"""
                    <tr>
                        <td>{row['cohort']}</td>
                        <td class="number">{row['cohort_age_days']}</td>
                        <td class="number">{row['total_customers']}</td>
                        <td class="number" style="font-weight: bold; color: #059669;">{row['retention_2nd_pct']}%</td>
                        <td class="number">{row['retention_3rd_pct']}%</td>
                        <td class="number">{row['retention_4th_pct']}%</td>
                        <td class="number">{row['retention_5th_pct']}%</td>
                        <td class="number">{row['avg_orders_per_customer']}</td>
                    </tr>"""

            # Add weighted average row
            html_content += f"""
                    <tr class="total-row">
                        <td>WEIGHTED AVERAGE</td>
                        <td class="number">-</td>
                        <td class="number">{mature_cohorts['total_customers'].sum()}</td>
                        <td class="number" style="font-weight: bold; color: #059669;">{true_retention_2nd}%</td>
                        <td class="number">{true_retention_3rd}%</td>
                        <td class="number">-</td>
                        <td class="number">-</td>
                        <td class="number">{mature_cohorts['avg_orders_per_customer'].mean():.2f}</td>
                    </tr>
                </tbody>
            </table>
            </div>
        </div>"""

    # === ITEM-BASED RETENTION ANALYSIS SECTIONS ===

    # 1. Retention by First Order Item Section
    if first_item_retention is not None:
        item_retention_df = first_item_retention.get('item_retention', pd.DataFrame())
        first_item_summary = first_item_retention.get('summary', {})

        if not item_retention_df.empty:
            html_content += f"""

        <h2 style="text-align: center; color: white; margin: 40px 0 20px; font-size: 2rem;">Retencia podľa prvého zakúpeného produktu</h2>
        <p style="text-align: center; color: white; margin-bottom: 20px; opacity: 0.8;">Porovnanie retencie zákazníkov podľa toho, aký produkt mali v prvej objednávke (top {len(item_retention_df)} produktov s min. 50 prvými objednávkami)</p>

        <div class="summary-cards" style="grid-template-columns: repeat(4, 1fr);">
            <div class="card">
                <div class="card-title">Analyzovaných produktov</div>
                <div class="card-value">{first_item_summary.get('total_items_analyzed', 0)}</div>
            </div>
            <div class="card">
                <div class="card-title">Priemerná 2nd retencia</div>
                <div class="card-value roi">{first_item_summary.get('avg_retention_2nd_pct', 0):.1f}%</div>
            </div>
            <div class="card">
                <div class="card-title">Najlepšia 2nd retencia</div>
                <div class="card-value profit">{first_item_summary.get('best_retention_2nd_pct', 0):.1f}%</div>
            </div>
            <div class="card">
                <div class="card-title">Rozptyl retencie</div>
                <div class="card-value">{first_item_summary.get('retention_spread', 0):.1f}%</div>
            </div>
        </div>

        <div class="chart-container">
            <h2 class="chart-title">Retencia podľa prvého produktu (2nd Order %)</h2>
            <p class="chart-explanation">Percentuálna pravdepodobnosť opätovného nákupu zákazníkov podľa toho, aký produkt si kúpili v prvej objednávke</p>
            <canvas id="firstItemRetentionChart"></canvas>
        </div>

        <div class="table-container">
            <div class="collapsible-header" onclick="toggleCollapse(this)">
                <h2 class="table-title">Detailná retencia podľa prvého produktu</h2>
                <span class="toggle-icon">▼</span>
            </div>
            <div class="collapsible-content">
            <table>
                <thead>
                    <tr>
                        <th>Produkt v prvej objednávke</th>
                        <th class="number">Zákazníci</th>
                        <th class="number">2nd Order %</th>
                        <th class="number">3rd Order %</th>
                        <th class="number">4th Order %</th>
                        <th class="number">5th Order %</th>
                        <th class="number">Priemer obj.</th>
                    </tr>
                </thead>
                <tbody>"""

            for _, row in item_retention_df.iterrows():
                item_name_short = row['item_name'][:45] + '...' if len(str(row['item_name'])) > 45 else row['item_name']
                html_content += f"""
                    <tr>
                        <td title="{row['item_name']}">{item_name_short}</td>
                        <td class="number">{row['first_order_customers']}</td>
                        <td class="number" style="font-weight: bold; color: #059669;">{row['retention_2nd_pct']}%</td>
                        <td class="number">{row['retention_3rd_pct']}%</td>
                        <td class="number">{row['retention_4th_pct']}%</td>
                        <td class="number">{row['retention_5th_pct']}%</td>
                        <td class="number">{row['avg_orders_per_customer']}</td>
                    </tr>"""

            # Add average row
            avg_retention_2nd = item_retention_df['retention_2nd_pct'].mean()
            avg_retention_3rd = item_retention_df['retention_3rd_pct'].mean()
            avg_retention_4th = item_retention_df['retention_4th_pct'].mean()
            avg_retention_5th = item_retention_df['retention_5th_pct'].mean()
            avg_orders = item_retention_df['avg_orders_per_customer'].mean()
            total_customers = item_retention_df['first_order_customers'].sum()

            html_content += f"""
                    <tr class="total-row">
                        <td>PRIEMER</td>
                        <td class="number">{total_customers}</td>
                        <td class="number" style="font-weight: bold;">{avg_retention_2nd:.1f}%</td>
                        <td class="number">{avg_retention_3rd:.1f}%</td>
                        <td class="number">{avg_retention_4th:.1f}%</td>
                        <td class="number">{avg_retention_5th:.1f}%</td>
                        <td class="number">{avg_orders:.2f}</td>
                    </tr>
                </tbody>
            </table>
            </div>
        </div>"""

    # 2. Time to Nth Order by First Item Section
    if time_to_nth_by_first_item is not None:
        time_to_nth_df = time_to_nth_by_first_item.get('time_to_nth_by_item', pd.DataFrame())
        time_nth_summary = time_to_nth_by_first_item.get('summary', {})

        if not time_to_nth_df.empty:
            html_content += f"""

        <h2 style="text-align: center; color: white; margin: 40px 0 20px; font-size: 2rem;">Čas do ďalšej objednávky podľa prvého produktu</h2>
        <p style="text-align: center; color: white; margin-bottom: 20px; opacity: 0.8;">Comparison of average time (in days) to 2nd, 3rd, and 4th order by first-order product.</p>

        <div class="summary-cards" style="grid-template-columns: repeat(4, 1fr);">
            <div class="card">
                <div class="card-title">Priemerný čas do 2. obj.</div>
                <div class="card-value">{time_nth_summary.get('avg_days_to_2nd_overall', 'N/A')} dní</div>
            </div>
            <div class="card">
                <div class="card-title">Najrýchlejší návrat</div>
                <div class="card-value profit">{time_nth_summary.get('fastest_return_days', 'N/A')} dní</div>
            </div>
            <div class="card">
                <div class="card-title">Najpomalší návrat</div>
                <div class="card-value cost">{time_nth_summary.get('slowest_return_days', 'N/A')} dní</div>
            </div>
            <div class="card">
                <div class="card-title">Rozptyl (dní)</div>
                <div class="card-value">{time_nth_summary.get('days_spread', 0):.1f}</div>
            </div>
        </div>

        <div class="chart-container">
            <h2 class="chart-title">Priemerný čas do 2. objednávky podľa prvého produktu</h2>
            <p class="chart-explanation">Zákazníci, ktorí si kúpili tento produkt v prvej objednávke, sa vrátili v priemere za uvedený počet dní</p>
            <canvas id="timeToNthByFirstItemChart"></canvas>
        </div>

        <div class="table-container">
            <div class="collapsible-header" onclick="toggleCollapse(this)">
                <h2 class="table-title">Čas do N-tej objednávky podľa prvého produktu</h2>
                <span class="toggle-icon">▼</span>
            </div>
            <div class="collapsible-content">
            <table>
                <thead>
                    <tr>
                        <th>Produkt v prvej objednávke</th>
                        <th class="number">Zákazníci</th>
                        <th class="number">Čas do 2. obj. (dní)</th>
                        <th class="number">Čas do 3. obj. (dní)</th>
                        <th class="number">Čas do 4. obj. (dní)</th>
                        <th class="number">Čas do 5. obj. (dní)</th>
                    </tr>
                </thead>
                <tbody>"""

            for _, row in time_to_nth_df.iterrows():
                item_name_short = row['item_name'][:45] + '...' if len(str(row['item_name'])) > 45 else row['item_name']
                avg_2nd = row.get('avg_days_to_2nd', '-')
                avg_3rd = row.get('avg_days_to_3rd', '-')
                avg_4th = row.get('avg_days_to_4th', '-')
                avg_5th = row.get('avg_days_to_5th', '-')
                html_content += f"""
                    <tr>
                        <td title="{row['item_name']}">{item_name_short}</td>
                        <td class="number">{row['first_order_customers']}</td>
                        <td class="number" style="font-weight: bold; color: #059669;">{avg_2nd if avg_2nd and str(avg_2nd) != 'nan' else '-'}</td>
                        <td class="number">{avg_3rd if avg_3rd and str(avg_3rd) != 'nan' else '-'}</td>
                        <td class="number">{avg_4th if avg_4th and str(avg_4th) != 'nan' else '-'}</td>
                        <td class="number">{avg_5th if avg_5th and str(avg_5th) != 'nan' else '-'}</td>
                    </tr>"""

            html_content += """
                </tbody>
            </table>
            </div>
        </div>"""

    # 3. Same Item Repurchase Section
    if same_item_repurchase is not None:
        item_repurchase_df = same_item_repurchase.get('item_repurchase', pd.DataFrame())
        repurchase_summary = same_item_repurchase.get('summary', {})

        if not item_repurchase_df.empty:
            html_content += f"""

        <h2 style="text-align: center; color: white; margin: 40px 0 20px; font-size: 2rem;">Opakovaný nákup rovnakého produktu</h2>
        <p style="text-align: center; color: white; margin-bottom: 20px; opacity: 0.8;">Analýza toho, ako často zákazníci kupujú ten istý produkt opakovane v ďalších objednávkach (top {len(item_repurchase_df)} produktov)</p>

        <div class="summary-cards" style="grid-template-columns: repeat(4, 1fr);">
            <div class="card">
                <div class="card-title">Priemerná 2x repurchase</div>
                <div class="card-value roi">{repurchase_summary.get('avg_repurchase_2x_pct', 0):.1f}%</div>
            </div>
            <div class="card">
                <div class="card-title">Najlepšia repurchase</div>
                <div class="card-value profit">{repurchase_summary.get('best_repurchase_2x_pct', 0):.1f}%</div>
            </div>
            <div class="card">
                <div class="card-title">Priem. dni medzi nákupmi</div>
                <div class="card-value">{repurchase_summary.get('avg_days_between_repurchase', 'N/A')} dní</div>
            </div>
            <div class="card">
                <div class="card-title">Rozptyl repurchase</div>
                <div class="card-value">{repurchase_summary.get('repurchase_spread', 0):.1f}%</div>
            </div>
        </div>

        <div class="chart-container">
            <h2 class="chart-title">Opakovaný nákup rovnakého produktu (2x+)</h2>
            <p class="chart-explanation">Percento zákazníkov, ktorí si kúpili tento produkt minimálne dvakrát v rôznych objednávkach</p>
            <canvas id="sameItemRepurchaseChart"></canvas>
        </div>

        <div class="table-container">
            <div class="collapsible-header" onclick="toggleCollapse(this)">
                <h2 class="table-title">Detailná analýza opakovaných nákupov produktov</h2>
                <span class="toggle-icon">▼</span>
            </div>
            <div class="collapsible-content">
            <table>
                <thead>
                    <tr>
                        <th>Produkt</th>
                        <th class="number">Celkom obj.</th>
                        <th class="number">Zákazníci</th>
                        <th class="number">2x+ %</th>
                        <th class="number">3x+ %</th>
                        <th class="number">4x+ %</th>
                        <th class="number">Priemer nák.</th>
                        <th class="number">Dni medzi</th>
                    </tr>
                </thead>
                <tbody>"""

            for _, row in item_repurchase_df.iterrows():
                item_name_short = row['item_name'][:40] + '...' if len(str(row['item_name'])) > 40 else row['item_name']
                days_between = row.get('avg_days_between_repurchase', '-')
                html_content += f"""
                    <tr>
                        <td title="{row['item_name']}">{item_name_short}</td>
                        <td class="number">{row['total_orders']}</td>
                        <td class="number">{row['unique_customers']}</td>
                        <td class="number" style="font-weight: bold; color: #059669;">{row['repurchase_2x_pct']}%</td>
                        <td class="number">{row['repurchase_3x_pct']}%</td>
                        <td class="number">{row['repurchase_4x_pct']}%</td>
                        <td class="number">{row['avg_purchases_per_customer']}</td>
                        <td class="number">{days_between if days_between and str(days_between) != 'nan' else '-'}</td>
                    </tr>"""

            # Add average row
            avg_repurchase_2x = item_repurchase_df['repurchase_2x_pct'].mean()
            avg_repurchase_3x = item_repurchase_df['repurchase_3x_pct'].mean()
            avg_repurchase_4x = item_repurchase_df['repurchase_4x_pct'].mean()
            avg_purchases = item_repurchase_df['avg_purchases_per_customer'].mean()
            total_orders = item_repurchase_df['total_orders'].sum()
            total_customers = item_repurchase_df['unique_customers'].sum()
            avg_days = item_repurchase_df[item_repurchase_df['avg_days_between_repurchase'].notna()]['avg_days_between_repurchase'].mean()
            avg_days_str = f"{avg_days:.1f}" if not pd.isna(avg_days) else '-'

            html_content += f"""
                    <tr class="total-row">
                        <td>PRIEMER</td>
                        <td class="number">{total_orders}</td>
                        <td class="number">{total_customers}</td>
                        <td class="number" style="font-weight: bold;">{avg_repurchase_2x:.1f}%</td>
                        <td class="number">{avg_repurchase_3x:.1f}%</td>
                        <td class="number">{avg_repurchase_4x:.1f}%</td>
                        <td class="number">{avg_purchases:.2f}</td>
                        <td class="number">{avg_days_str}</td>
                    </tr>
                </tbody>
            </table>
            </div>
        </div>"""

    html_content += """

        <div class="table-container">
            <div class="collapsible-header" onclick="toggleCollapse(this)">
                <h2 class="table-title">Daily Performance Summary</h2>
                <span class="toggle-icon">▼</span>
            </div>
            <div class="collapsible-content">
            <table>
                <thead>
                    <tr>
                        <th>Date</th>
                        <th class="number">Orders</th>
                        <th class="number">Revenue</th>
                        <th class="number">AOV</th>
                        <th class="number">Avg Items/Order</th>
                        <th class="number">Product Costs</th>
                        <th class="number">Packaging + Shipping + Fixed</th>
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
        fixed_costs = row['packaging_cost'] + row.get('shipping_subsidy_cost', 0) + row['fixed_daily_cost']
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
        </div>

        <div class="table-container">
            <div class="collapsible-header" onclick="toggleCollapse(this)">
                <h2 class="table-title">All Products by Revenue (Product Costs Only)</h2>
                <span class="toggle-icon">▼</span>
            </div>
            <div class="collapsible-content">
            <table>
                <thead>
                    <tr>
                        <th>Product</th>
                        <th>SKU/EAN</th>
                        <th class="number">Quantity</th>
                        <th class="number">Revenue</th>
                        <th class="number">Product Cost</th>
                        <th class="number">Profit</th>
                        <th class="number">ROI %</th>
                        <th class="number">Share (Items / Revenue / Profit)</th>
                    </tr>
                </thead>
                <tbody>
"""

    # Add all products
    for _, row in all_products.iterrows():
        profit_class = "profit-positive" if row['profit'] > 0 else "profit-negative"
        product_name = row['product_name'][:50] + '...' if len(row['product_name']) > 50 else row['product_name']
        product_sku = row.get('product_sku', '') if pd.notna(row.get('product_sku', '')) else ''

        # Calculate share percentages
        quantity_share = (row['total_quantity'] / total_all_products_quantity * 100) if total_all_products_quantity > 0 else 0
        revenue_share = (row['total_revenue'] / total_all_products_revenue * 100) if total_all_products_revenue > 0 else 0
        profit_share = (row['profit'] / total_all_products_profit * 100) if total_all_products_profit > 0 else 0

        html_content += f"""
                    <tr>
                        <td>{product_name}</td>
                        <td>{product_sku}</td>
                        <td class="number">{row['total_quantity']}</td>
                        <td class="number">€{row['total_revenue']:,.2f}</td>
                        <td class="number">€{row['product_expense']:,.2f}</td>
                        <td class="number {profit_class}">€{row['profit']:,.2f}</td>
                        <td class="number">{row['roi_percent']:.1f}%</td>
                        <td class="number">{quantity_share:.1f}% / {revenue_share:.1f}% / {profit_share:.1f}%</td>
                    </tr>
"""
    
    html_content += f"""
                </tbody>
            </table>
            </div>
        </div>"""

    # Add item combinations section if data is available
    if item_combinations is not None and not item_combinations.empty:
        # Group by combination size for the table
        html_content += """

        <h2 style="text-align: center; color: white; margin: 40px 0 20px; font-size: 2rem;">Frequently Ordered Item Combinations</h2>
        <p style="text-align: center; color: white; margin-bottom: 20px; opacity: 0.8;">Combinations of items that appear together in orders at least 5 times (order of items doesn't matter)</p>

        <div class="chart-container">
            <h2 class="chart-title">Top Item Combinations by Frequency</h2>
            <p class="chart-explanation">Shows how many times each combination of products was ordered together. Higher values indicate popular product bundles.</p>
            <canvas id="itemCombinationsChart"></canvas>
        </div>

        <div class="table-container">
            <div class="collapsible-header" onclick="toggleCollapse(this)">
                <h2 class="table-title">Item Combinations Analysis</h2>
                <span class="toggle-icon">▼</span>
            </div>
            <div class="collapsible-content">
            <table>
                <thead>
                    <tr>
                        <th>Combination Size</th>
                        <th>Product Combination</th>
                        <th class="number">Times Ordered Together</th>
                        <th class="number">Combination Price</th>
                    </tr>
                </thead>
                <tbody>"""

        # Add rows for each combination, grouped by size
        for _, row in item_combinations.iterrows():
            # Convert newlines to <br> tags for HTML display
            combo_display = row['combination'].replace('\n', '<br>')
            combo_price = row.get('price', 0)
            html_content += f"""
                    <tr>
                        <td>{row['combination_size']} items</td>
                        <td>{combo_display}</td>
                        <td class="number">{row['count']}</td>
                        <td class="number">€{combo_price:.2f}</td>
                    </tr>"""

        html_content += """
                </tbody>
            </table>
            </div>
        </div>"""

    # ===== NEW ANALYTICS SECTIONS =====

    # Day of Week Analysis
    if day_of_week_analysis is not None and not day_of_week_analysis.empty:
        dow_labels = day_of_week_analysis['day_name'].tolist()
        dow_orders = day_of_week_analysis['orders'].tolist()
        dow_revenue = day_of_week_analysis['revenue'].tolist()
        dow_aov = day_of_week_analysis['aov'].tolist()

        html_content += f"""

        <h2 style="text-align: center; color: white; margin: 40px 0 20px; font-size: 2rem;">Day of Week Analysis</h2>

        <div class="chart-grid">
            <div class="chart-container">
                <h2 class="chart-title">Orders & FB Spend by Day of Week</h2>
                <p class="chart-explanation">Orders (bars) and FB ad spend (line) by day. Compare cost per order across days.</p>
                <canvas id="dowOrdersChart"></canvas>
            </div>
            <div class="chart-container">
                <h2 class="chart-title">Revenue & FB Spend by Day of Week</h2>
                <p class="chart-explanation">Revenue (bars) and FB ad spend (line) by day of week. Compare spending efficiency across days.</p>
                <canvas id="dowRevenueChart"></canvas>
            </div>
        </div>

        <div class="table-container">
            <div class="collapsible-header" onclick="toggleCollapse(this)">
                <h2 class="table-title">Day of Week Performance</h2>
                <span class="toggle-icon">▼</span>
            </div>
            <div class="collapsible-content">
            <table>
                <thead>
                    <tr>
                        <th>Day</th>
                        <th class="number">Orders</th>
                        <th class="number">Orders %</th>
                        <th class="number">Revenue</th>
                        <th class="number">Revenue %</th>
                        <th class="number">AOV</th>
                    </tr>
                </thead>
                <tbody>"""

        for _, row in day_of_week_analysis.iterrows():
            html_content += f"""
                    <tr>
                        <td>{row['day_name']}</td>
                        <td class="number">{row['orders']}</td>
                        <td class="number">{row['orders_pct']:.1f}%</td>
                        <td class="number">€{row['revenue']:,.2f}</td>
                        <td class="number">{row['revenue_pct']:.1f}%</td>
                        <td class="number">€{row['aov']:.2f}</td>
                    </tr>"""

        html_content += """
                </tbody>
            </table>
            </div>
        </div>"""

    # Day/Hour Heatmap (add after day of week analysis)
    if day_hour_heatmap is not None and not day_hour_heatmap.empty:
        # Prepare heatmap data as JSON for JavaScript
        heatmap_json = day_hour_heatmap.to_dict('records')
        max_orders = day_hour_heatmap['orders'].max()

        html_content += f"""

        <div class="chart-container">
            <h2 class="chart-title">Orders Heatmap: Day of Week × Hour of Day</h2>
            <p class="chart-explanation">Shows when customers place orders. Darker colors = more orders. Helps identify peak shopping times for ad scheduling and staffing.</p>
            <div id="heatmapContainer" style="overflow-x: auto;">
                <table class="heatmap-table" style="width: 100%; border-collapse: collapse; margin-top: 20px;">
                    <thead>
                        <tr>
                            <th style="padding: 8px; background: #f7fafc; color: #4a5568; font-weight: 600; text-align: left; border: 1px solid #e2e8f0;">Day / Hour</th>"""

        # Add hour headers (0-23)
        for hour in range(24):
            html_content += f"""
                            <th style="padding: 4px; background: #f7fafc; color: #4a5568; font-weight: 600; text-align: center; border: 1px solid #e2e8f0; min-width: 30px; font-size: 0.8rem;">{hour:02d}</th>"""

        html_content += """
                        </tr>
                    </thead>
                    <tbody>"""

        # Add rows for each day
        day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        for day_idx, day_name in enumerate(day_names):
            html_content += f"""
                        <tr>
                            <td style="padding: 8px; background: #f7fafc; color: #4a5568; font-weight: 600; border: 1px solid #e2e8f0;">{day_name}</td>"""

            for hour in range(24):
                # Find the order count for this day/hour
                row_data = day_hour_heatmap[(day_hour_heatmap['day_of_week'] == day_idx) & (day_hour_heatmap['hour'] == hour)]
                orders = int(row_data['orders'].values[0]) if len(row_data) > 0 else 0

                # Calculate color intensity (0 to 1)
                intensity = orders / max_orders if max_orders > 0 else 0

                # Color gradient from light to dark (using purple gradient)
                if orders == 0:
                    bg_color = "#f8f9fa"
                    text_color = "#999"
                else:
                    # Gradient from light purple to dark purple
                    r = int(248 - (248 - 102) * intensity)
                    g = int(249 - (249 - 126) * intensity)
                    b = int(250 - (250 - 234) * intensity)
                    bg_color = f"rgb({r}, {g}, {b})"
                    text_color = "#fff" if intensity > 0.5 else "#333"

                html_content += f"""
                            <td style="padding: 4px; text-align: center; background: {bg_color}; color: {text_color}; border: 1px solid #e2e8f0; font-size: 0.75rem; font-weight: {'600' if orders > 0 else '400'};">{orders if orders > 0 else ''}</td>"""

            html_content += """
                        </tr>"""

        html_content += """
                    </tbody>
                </table>
            </div>
            <div style="margin-top: 15px; text-align: center;">
                <div style="display: inline-flex; align-items: center; gap: 10px; font-size: 0.85rem; color: #718096;">
                    <span>Less orders</span>
                    <div style="display: flex; gap: 2px;">
                        <div style="width: 20px; height: 15px; background: #f8f9fa; border: 1px solid #e2e8f0;"></div>
                        <div style="width: 20px; height: 15px; background: rgb(211, 215, 244);"></div>
                        <div style="width: 20px; height: 15px; background: rgb(175, 181, 238);"></div>
                        <div style="width: 20px; height: 15px; background: rgb(138, 147, 232);"></div>
                        <div style="width: 20px; height: 15px; background: rgb(102, 126, 234);"></div>
                    </div>
                    <span>More orders</span>
                </div>
            </div>
        </div>"""

    # Geographic Analysis
    if country_analysis is not None and not country_analysis.empty:
        html_content += """

        <h2 style="text-align: center; color: white; margin: 40px 0 20px; font-size: 2rem;">Geographic Analysis</h2>

        <div class="chart-container">
            <h2 class="chart-title">Revenue by Country</h2>
            <p class="chart-explanation">Distribution of revenue across different countries.</p>
            <canvas id="countryChart"></canvas>
        </div>

        <div class="table-container">
            <div class="collapsible-header" onclick="toggleCollapse(this)">
                <h2 class="table-title">Revenue by Country</h2>
                <span class="toggle-icon">▼</span>
            </div>
            <div class="collapsible-content">
            <table>
                <thead>
                    <tr>
                        <th>Country</th>
                        <th class="number">Orders</th>
                        <th class="number">Revenue</th>
                        <th class="number">Revenue %</th>
                        <th class="number">Profit</th>
                    </tr>
                </thead>
                <tbody>"""

        for _, row in country_analysis.iterrows():
            html_content += f"""
                    <tr>
                        <td>{row['country']}</td>
                        <td class="number">{row['orders']}</td>
                        <td class="number">€{row['revenue']:,.2f}</td>
                        <td class="number">{row['revenue_pct']:.1f}%</td>
                        <td class="number">€{row['profit']:,.2f}</td>
                    </tr>"""

        html_content += """
                </tbody>
            </table>
            </div>
        </div>"""

    if geo_profitability and isinstance(geo_profitability, dict):
        geo_table = geo_profitability.get('table')
        unattributed_fb = geo_profitability.get('fb_spend_unattributed', 0)
        if geo_table is not None and not geo_table.empty:
            html_content += f"""

        <div class="chart-container">
            <h2 class="chart-title">SK/CZ/HU Profitability (Post-Ad Contribution + FB CPO)</h2>
            <p class="chart-explanation">Country-level post-ad contribution view using net revenue, product costs, packaging, shipping subsidy, and estimated FB spend by campaign naming (fixed overhead excluded). Unattributed FB spend (not mapped to SK/CZ/HU): €{unattributed_fb:,.2f}.</p>
            <canvas id="geoProfitabilityChart"></canvas>
        </div>

        <div class="table-container">
            <div class="collapsible-header" onclick="toggleCollapse(this)">
                <h2 class="table-title">Geo Profitability (SK/CZ/HU)</h2>
                <span class="toggle-icon">▼</span>
            </div>
            <div class="collapsible-content">
            <table>
                <thead>
                    <tr>
                        <th>Country</th>
                        <th class="number">Orders</th>
                        <th class="number">Revenue</th>
                        <th class="number">Product Cost</th>
                        <th class="number">Packaging</th>
                        <th class="number">Shipping Subsidy</th>
                        <th class="number">FB Spend</th>
                        <th class="number">Post-Ad Contribution Profit</th>
                        <th class="number">Post-Ad Contribution Margin %</th>
                        <th class="number">FB CPO</th>
                    </tr>
                </thead>
                <tbody>"""

            for _, row in geo_table.iterrows():
                html_content += f"""
                    <tr>
                        <td>{str(row.get('country', '')).upper()}</td>
                        <td class="number">{int(row.get('orders', 0))}</td>
                        <td class="number">€{row.get('revenue', 0):,.2f}</td>
                        <td class="number">€{row.get('product_cost', 0):,.2f}</td>
                        <td class="number">€{row.get('packaging_cost', 0):,.2f}</td>
                        <td class="number">€{row.get('shipping_subsidy_cost', 0):,.2f}</td>
                        <td class="number">€{row.get('fb_ads_spend', 0):,.2f}</td>
                        <td class="number {'profit-positive' if row.get('contribution_profit', 0) >= 0 else 'profit-negative'}">€{row.get('contribution_profit', 0):,.2f}</td>
                        <td class="number {'profit-positive' if row.get('contribution_margin_pct', 0) >= 0 else 'profit-negative'}">{row.get('contribution_margin_pct', 0):.2f}%</td>
                        <td class="number">€{row.get('fb_cpo', 0):,.2f}</td>
                    </tr>"""

            html_content += """
                </tbody>
            </table>
            </div>
        </div>"""

    # Top Cities
    if city_analysis is not None and not city_analysis.empty:
        html_content += """

        <div class="table-container">
            <div class="collapsible-header" onclick="toggleCollapse(this)">
                <h2 class="table-title">Top 20 Cities by Revenue</h2>
                <span class="toggle-icon">▼</span>
            </div>
            <div class="collapsible-content">
            <table>
                <thead>
                    <tr>
                        <th>City</th>
                        <th>Country</th>
                        <th class="number">Orders</th>
                        <th class="number">Revenue</th>
                        <th class="number">Revenue %</th>
                    </tr>
                </thead>
                <tbody>"""

        for _, row in city_analysis.iterrows():
            html_content += f"""
                    <tr>
                        <td>{row['city']}</td>
                        <td>{row['country']}</td>
                        <td class="number">{row['orders']}</td>
                        <td class="number">€{row['revenue']:,.2f}</td>
                        <td class="number">{row['revenue_pct']:.1f}%</td>
                    </tr>"""

        html_content += """
                </tbody>
            </table>
            </div>
        </div>"""

    # B2B vs B2C Analysis
    if b2b_analysis is not None and not b2b_analysis.empty:
        html_content += """

        <h2 style="text-align: center; color: white; margin: 40px 0 20px; font-size: 2rem;">B2B vs B2C Analysis</h2>

        <div class="chart-container">
            <h2 class="chart-title">B2B vs B2C Revenue Split</h2>
            <p class="chart-explanation">Comparison of business customers (with VAT/Company ID) vs individual consumers.</p>
            <canvas id="b2bChart"></canvas>
        </div>

        <div class="table-container">
            <div class="collapsible-header" onclick="toggleCollapse(this)">
                <h2 class="table-title">B2B vs B2C Performance</h2>
                <span class="toggle-icon">▼</span>
            </div>
            <div class="collapsible-content">
            <table>
                <thead>
                    <tr>
                        <th>Customer Type</th>
                        <th class="number">Orders</th>
                        <th class="number">Orders %</th>
                        <th class="number">Revenue</th>
                        <th class="number">Revenue %</th>
                        <th class="number">AOV</th>
                        <th class="number">Unique Customers</th>
                    </tr>
                </thead>
                <tbody>"""

        for _, row in b2b_analysis.iterrows():
            html_content += f"""
                    <tr>
                        <td>{row['customer_type']}</td>
                        <td class="number">{row['orders']}</td>
                        <td class="number">{row['orders_pct']:.1f}%</td>
                        <td class="number">€{row['revenue']:,.2f}</td>
                        <td class="number">{row['revenue_pct']:.1f}%</td>
                        <td class="number">€{row['aov']:.2f}</td>
                        <td class="number">{row['unique_customers']}</td>
                    </tr>"""

        html_content += """
                </tbody>
            </table>
            </div>
        </div>"""

    # Customer Concentration
    if customer_concentration:
        top_10_pct = customer_concentration.get('top_10_pct_revenue_share', 0)
        top_20_pct = customer_concentration.get('top_20_pct_revenue_share', 0)
        total_customers = customer_concentration.get('total_customers', 0)
        repeat_customers = customer_concentration.get('repeat_customers', 0)
        one_time = customer_concentration.get('one_time_customers', 0)
        avg_rev = customer_concentration.get('avg_revenue_per_customer', 0)
        median_rev = customer_concentration.get('median_revenue_per_customer', 0)

        html_content += f"""

        <h2 style="text-align: center; color: white; margin: 40px 0 20px; font-size: 2rem;">Customer Analysis</h2>

        <div class="summary-cards">
            <div class="card">
                <div class="card-title">Total Customers</div>
                <div class="card-value">{total_customers}</div>
            </div>
            <div class="card">
                <div class="card-title">Top 10% Revenue Share</div>
                <div class="card-value">{top_10_pct:.1f}%</div>
            </div>
            <div class="card">
                <div class="card-title">Top 20% Revenue Share</div>
                <div class="card-value">{top_20_pct:.1f}%</div>
            </div>
            <div class="card">
                <div class="card-title">Repeat Customers</div>
                <div class="card-value roi">{repeat_customers}</div>
            </div>
            <div class="card">
                <div class="card-title">One-time Customers</div>
                <div class="card-value">{one_time}</div>
            </div>
            <div class="card">
                <div class="card-title">Avg Revenue/Customer</div>
                <div class="card-value">€{avg_rev:.2f}</div>
            </div>
            <div class="card">
                <div class="card-title">Median Revenue/Customer</div>
                <div class="card-value">€{median_rev:.2f}</div>
            </div>
        </div>

        <div class="chart-container">
            <h2 class="chart-title">Customer Concentration</h2>
            <p class="chart-explanation">Shows how much of your revenue comes from top customers. High concentration = risk if top customers leave.</p>
            <canvas id="customerConcentrationChart"></canvas>
        </div>"""

        # Top 10 customers table
        top_customers = customer_concentration.get('top_10_customers')
        if top_customers is not None and not top_customers.empty:
            html_content += """

        <div class="table-container">
            <div class="collapsible-header" onclick="toggleCollapse(this)">
                <h2 class="table-title">Top 10 Customers by Revenue</h2>
                <span class="toggle-icon">▼</span>
            </div>
            <div class="collapsible-content">
            <table>
                <thead>
                    <tr>
                        <th>Customer</th>
                        <th class="number">Orders</th>
                        <th class="number">Revenue</th>
                        <th class="number">% of Total</th>
                        <th class="number">Profit</th>
                    </tr>
                </thead>
                <tbody>"""

            for _, row in top_customers.iterrows():
                customer_display = row['customer'][:40] + '...' if len(str(row['customer'])) > 40 else row['customer']
                html_content += f"""
                    <tr>
                        <td>{customer_display}</td>
                        <td class="number">{row['orders']}</td>
                        <td class="number">€{row['revenue']:,.2f}</td>
                        <td class="number">{row['revenue_pct']:.1f}%</td>
                        <td class="number">€{row['profit']:,.2f}</td>
                    </tr>"""

            html_content += """
                </tbody>
            </table>
            </div>
        </div>"""

    # Product Margins
    if product_margins is not None and not product_margins.empty:
        html_content += """

        <h2 style="text-align: center; color: white; margin: 40px 0 20px; font-size: 2rem;">Product Margin Analysis</h2>

        <div class="chart-container">
            <h2 class="chart-title">Product Margins (Top 20)</h2>
            <p class="chart-explanation">Profit margin percentage by product. Higher margins = more profitable products.</p>
            <canvas id="marginChart"></canvas>
        </div>

        <div class="table-container">
            <div class="collapsible-header" onclick="toggleCollapse(this)">
                <h2 class="table-title">Product Margins (Sorted by Margin %)</h2>
                <span class="toggle-icon">▼</span>
            </div>
            <div class="collapsible-content">
            <table>
                <thead>
                    <tr>
                        <th>Product</th>
                        <th class="number">Quantity</th>
                        <th class="number">Revenue</th>
                        <th class="number">Cost</th>
                        <th class="number">Profit</th>
                        <th class="number">Margin %</th>
                    </tr>
                </thead>
                <tbody>"""

        for _, row in product_margins.head(50).iterrows():
            product_display = row['product'][:50] + '...' if len(str(row['product'])) > 50 else row['product']
            margin_class = "profit-positive" if row['margin_pct'] > 0 else "profit-negative"
            html_content += f"""
                    <tr>
                        <td>{product_display}</td>
                        <td class="number">{row['quantity']}</td>
                        <td class="number">€{row['revenue']:,.2f}</td>
                        <td class="number">€{row['cost']:,.2f}</td>
                        <td class="number">€{row['profit']:,.2f}</td>
                        <td class="number {margin_class}">{row['margin_pct']:.1f}%</td>
                    </tr>"""

        html_content += """
                </tbody>
            </table>
            </div>
        </div>"""

    # Product Trends
    if product_trends is not None and not product_trends.empty:
        html_content += """

        <h2 style="text-align: center; color: white; margin: 40px 0 20px; font-size: 2rem;">Product Trend Analysis</h2>
        <p style="text-align: center; color: white; margin-bottom: 20px; opacity: 0.8;">Comparing first half vs second half of the period to identify growing and declining products.</p>

        <div class="table-container">
            <div class="collapsible-header" onclick="toggleCollapse(this)">
                <h2 class="table-title">Product Trends (by Total Revenue)</h2>
                <span class="toggle-icon">▼</span>
            </div>
            <div class="collapsible-content">
            <table>
                <thead>
                    <tr>
                        <th>Product</th>
                        <th class="number">1st Half Qty</th>
                        <th class="number">2nd Half Qty</th>
                        <th class="number">Growth %</th>
                        <th>Trend</th>
                        <th class="number">Total Revenue</th>
                    </tr>
                </thead>
                <tbody>"""

        for _, row in product_trends.head(50).iterrows():
            product_display = row['product'][:40] + '...' if len(str(row['product'])) > 40 else row['product']
            trend = row['trend']
            if trend == 'Growing':
                trend_class = "profit-positive"
            elif trend == 'Declining':
                trend_class = "profit-negative"
            elif trend == 'New':
                trend_class = "roi"
            else:
                trend_class = ""
            growth_class = "profit-positive" if row['qty_growth_pct'] > 0 else "profit-negative"
            html_content += f"""
                    <tr>
                        <td>{product_display}</td>
                        <td class="number">{int(row['qty_first'])}</td>
                        <td class="number">{int(row['qty_second'])}</td>
                        <td class="number {growth_class}">{row['qty_growth_pct']:.1f}%</td>
                        <td class="{trend_class}">{trend}</td>
                        <td class="number">€{row['total_revenue']:,.2f}</td>
                    </tr>"""

        html_content += """
                </tbody>
            </table>
            </div>
        </div>"""

    # Ads Effectiveness Analysis
    if ads_effectiveness:
        correlations = ads_effectiveness.get('correlations', {})
        spend_effectiveness = ads_effectiveness.get('spend_effectiveness')
        dow_effectiveness = ads_effectiveness.get('dow_effectiveness')
        recommendations = ads_effectiveness.get('recommendations', [])
        best_roas = ads_effectiveness.get('best_roas_range', 'N/A')
        best_profit = ads_effectiveness.get('best_profit_range', 'N/A')

        html_content += f"""

        <h2 style="text-align: center; color: white; margin: 40px 0 20px; font-size: 2rem;">Ads Effectiveness Analysis</h2>

        <div class="summary-cards">
            <div class="card">
                <div class="card-title">FB Spend ↔ Orders</div>
                <div class="card-value">{correlations.get('fb_orders', 0):.3f}</div>
            </div>
            <div class="card">
                <div class="card-title">FB Spend ↔ Revenue</div>
                <div class="card-value">{correlations.get('fb_revenue', 0):.3f}</div>
            </div>
            <div class="card">
                <div class="card-title">Best ROAS Spend Level</div>
                <div class="card-value roi">{best_roas}</div>
            </div>
            <div class="card">
                <div class="card-title">Best Profit Spend Level</div>
                <div class="card-value profit">{best_profit}</div>
            </div>
        </div>

        <div class="chart-grid">
            <div class="chart-container">
                <h2 class="chart-title">Ad Spend vs Orders Correlation</h2>
                <p class="chart-explanation">Scatter plot showing relationship between daily FB spend and number of orders. Green = profitable day, Red = loss day.</p>
                <canvas id="adsOrdersChart"></canvas>
            </div>

            <div class="chart-container">
                <h2 class="chart-title">Ad Spend vs Revenue Correlation</h2>
                <p class="chart-explanation">Scatter plot showing relationship between daily FB spend and revenue. Green = profitable day, Red = loss day.</p>
                <canvas id="adsRevenueChart"></canvas>
            </div>
        </div>

        <div class="chart-grid">
            <div class="chart-container">
                <h2 class="chart-title">Total Cost vs Revenue Correlation</h2>
                <p class="chart-explanation">Scatter plot showing relationship between daily total costs (product + packaging + shipping subsidy + ads + fixed) and revenue. Higher correlation indicates predictable cost-revenue relationship.</p>
                <canvas id="costRevenueChart"></canvas>
            </div>
        </div>

        <div class="chart-grid">
            <div class="chart-container">
                <h2 class="chart-title">FB Spend vs Orders by Range</h2>
                <p class="chart-explanation">Average orders for each FB spend range. Find the optimal spend level for maximizing orders.</p>
                <canvas id="spendRangeOrdersChart"></canvas>
            </div>
            <div class="chart-container">
                <h2 class="chart-title">FB Spend vs Revenue by Range</h2>
                <p class="chart-explanation">Average revenue and ROAS for each FB spend range. Find the optimal spend level for maximizing revenue.</p>
                <canvas id="spendRangeRevenueChart"></canvas>
            </div>
        </div>"""

        if recommendations:
            html_content += """
        <div class="table-container" style="background: #e6fffa;">
            <h2 class="table-title" style="color: #047857;">Recommendations</h2>
            <ul style="padding-left: 20px; color: #065f46;">"""
            for rec in recommendations:
                html_content += f"""
                <li style="margin: 10px 0;">{rec}</li>"""
            html_content += """
            </ul>
        </div>"""

        # Spend effectiveness table
        if spend_effectiveness is not None and not spend_effectiveness.empty:
            html_content += """

        <div class="table-container">
            <div class="collapsible-header" onclick="toggleCollapse(this)">
                <h2 class="table-title">FB Spend Effectiveness by Range</h2>
                <span class="toggle-icon">▼</span>
            </div>
            <div class="collapsible-content">
            <table>
                <thead>
                    <tr>
                        <th>Spend Range</th>
                        <th class="number">Avg Spend</th>
                        <th class="number">Avg Orders</th>
                        <th class="number">Avg Revenue</th>
                        <th class="number">Avg Profit</th>
                        <th class="number">ROAS</th>
                    </tr>
                </thead>
                <tbody>"""

            for _, row in spend_effectiveness.iterrows():
                html_content += f"""
                    <tr>
                        <td>{row['spend_range']}</td>
                        <td class="number">€{row['avg_spend']:.2f}</td>
                        <td class="number">{row['avg_orders']:.1f}</td>
                        <td class="number">€{row['avg_revenue']:.2f}</td>
                        <td class="number">€{row['avg_profit']:.2f}</td>
                        <td class="number">{row['roas']:.2f}x</td>
                    </tr>"""

            html_content += """
                </tbody>
            </table>
            </div>
        </div>"""

        # Day of week ad effectiveness
        if dow_effectiveness is not None and not dow_effectiveness.empty:
            html_content += """

        <div class="table-container">
            <div class="collapsible-header" onclick="toggleCollapse(this)">
                <h2 class="table-title">Ad Effectiveness by Day of Week</h2>
                <span class="toggle-icon">▼</span>
            </div>
            <div class="collapsible-content">
            <table>
                <thead>
                    <tr>
                        <th>Day</th>
                        <th class="number">Avg FB Spend</th>
                        <th class="number">Avg Orders</th>
                        <th class="number">Avg Revenue</th>
                        <th class="number">ROAS</th>
                    </tr>
                </thead>
                <tbody>"""

            for _, row in dow_effectiveness.iterrows():
                html_content += f"""
                    <tr>
                        <td>{row['day_of_week']}</td>
                        <td class="number">€{row['fb_spend']:.2f}</td>
                        <td class="number">{row['orders']:.1f}</td>
                        <td class="number">€{row['revenue']:.2f}</td>
                        <td class="number">{row['roas']:.2f}x</td>
                    </tr>"""

            html_content += """
                </tbody>
            </table>
            </div>
        </div>"""

    # Order Status Distribution
    if order_status is not None and not order_status.empty:
        html_content += """

        <h2 style="text-align: center; color: white; margin: 40px 0 20px; font-size: 2rem;">Order Status Distribution</h2>

        <div class="chart-container">
            <h2 class="chart-title">Orders by Status</h2>
            <p class="chart-explanation">Distribution of orders across different statuses.</p>
            <canvas id="statusChart"></canvas>
        </div>

        <div class="table-container">
            <div class="collapsible-header" onclick="toggleCollapse(this)">
                <h2 class="table-title">Order Status Breakdown</h2>
                <span class="toggle-icon">▼</span>
            </div>
            <div class="collapsible-content">
            <table>
                <thead>
                    <tr>
                        <th>Status</th>
                        <th class="number">Orders</th>
                        <th class="number">Orders %</th>
                        <th class="number">Revenue</th>
                    </tr>
                </thead>
                <tbody>"""

        for _, row in order_status.iterrows():
            html_content += f"""
                    <tr>
                        <td>{row['status']}</td>
                        <td class="number">{row['orders']}</td>
                        <td class="number">{row['orders_pct']:.1f}%</td>
                        <td class="number">€{row['revenue']:,.2f}</td>
                    </tr>"""

        html_content += """
                </tbody>
            </table>
            </div>
        </div>"""

    # Customer Email Segmentation Tables
    if customer_email_segments:
        html_content += """

        <h2 style="text-align: center; color: white; margin: 40px 0 20px; font-size: 2rem;">📧 Segmentácia zákazníkov pre email marketing</h2>
        <p style="text-align: center; color: white; margin-bottom: 20px; opacity: 0.9;">Tabuľky zákazníkov rozdelených podľa nákupného správania. Každý segment je vhodný na iný typ emailovej kampane.</p>
"""

        # Define segment display order and styling
        segment_configs = {
            'sample_not_converted': {'color': '#EC4899', 'icon': '🧪', 'priority': 1},
            'second_order_encouragement': {'color': '#8B5CF6', 'icon': '2ď¸ŹâŁ', 'priority': 2},
            'optimal_reorder_timing': {'color': '#10B981', 'icon': '🎯', 'priority': 3},
            'churning_customers': {'color': '#F97316', 'icon': '⚠️', 'priority': 4},
            'repeat_buyers_90_days': {'color': '#EF4444', 'icon': '🔄', 'priority': 5},
            'one_time_buyers_30_days': {'color': '#F59E0B', 'icon': '🛒', 'priority': 6},
            'high_value_one_time': {'color': '#06B6D4', 'icon': '💎', 'priority': 7},
            'new_customers_welcome': {'color': '#22C55E', 'icon': '👋', 'priority': 8},
            'vip_customers': {'color': '#A855F7', 'icon': '👑', 'priority': 9},
            'failed_payment_only': {'color': '#DC2626', 'icon': '❌', 'priority': 10},
            'recent_buyers_14_60_days': {'color': '#3B82F6', 'icon': '⏰', 'priority': 11},
            'long_dormant': {'color': '#6B7280', 'icon': '💤', 'priority': 12}
        }

        # First, show Email Campaign Calendar
        html_content += """
        <div class="table-container" style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); margin-bottom: 30px;">
            <h2 class="table-title" style="color: white;">📅 Plán emailových kampaní - Kedy komu poslať</h2>
            <table style="background: rgba(255,255,255,0.95);">
                <thead>
                    <tr>
                        <th>Priorita</th>
                        <th>Segment</th>
                        <th class="number">Počet</th>
                        <th>Kedy poslať</th>
                        <th>Odporúčaná zľava</th>
                        <th>Šablóna emailu</th>
                    </tr>
                </thead>
                <tbody>"""

        # Sort by priority from segment info
        priority_sorted = sorted(customer_email_segments.items(),
                                key=lambda x: x[1].get('priority', 99))

        for segment_name, segment_info in priority_sorted:
            if segment_info['count'] == 0:
                continue
            config = segment_configs.get(segment_name, {'color': '#6B7280', 'icon': '📋'})
            priority = segment_info.get('priority', 99)
            timing = segment_info.get('send_timing', 'Nie je definované')
            discount = segment_info.get('discount_suggestion', '-')
            template = segment_info.get('email_template', '-')

            priority_badge = '🔴' if priority <= 2 else ('🟡' if priority <= 4 else '🟢')

            html_content += f"""
                    <tr>
                        <td style="text-align: center;">{priority_badge} {priority}</td>
                        <td><span style="color: {config['color']}; font-weight: bold;">{config['icon']} {segment_info.get('description_en', segment_name)[:60]}...</span></td>
                        <td class="number" style="font-weight: bold; color: {config['color']};">{segment_info['count']}</td>
                        <td style="font-weight: 500;">{timing}</td>
                        <td>{discount}</td>
                        <td style="font-size: 0.85rem; font-style: italic;">"{template[:50]}..."</td>
                    </tr>"""

        html_content += """
                </tbody>
            </table>
            <p style="color: white; padding: 15px; font-size: 0.9rem;">
                <strong>🔴 Vysoká priorita</strong> = Poslať ihneď |
                <strong>🟡 Stredná priorita</strong> = Naplánované kampane |
                <strong>🟢 Nízka priorita</strong> = Pravidelné kampane
            </p>
        </div>
"""

        # Sort segments by priority
        sorted_segments = sorted(customer_email_segments.items(),
                                key=lambda x: segment_configs.get(x[0], {}).get('priority', 99))

        for segment_name, segment_info in sorted_segments:
            segment_data = segment_info['data']
            config = segment_configs.get(segment_name, {'color': '#6B7280', 'icon': '📋'})

            if segment_data is not None and not segment_data.empty:
                # Determine columns based on segment type
                if segment_name == 'failed_payment_only':
                    columns = ['email', 'name', 'failed_order_count', 'last_attempt_date', 'city', 'country']
                    headers = ['Email', 'Meno', 'Počet pokusov', 'Posledný pokus', 'Mesto', 'Krajina']
                else:
                    columns = ['email', 'name', 'order_count', 'total_revenue', 'days_since_last_order', 'city', 'country']
                    headers = ['Email', 'Meno', 'Počet obj.', 'Celková tržba', 'Dní od posl. obj.', 'Mesto', 'Krajina']

                html_content += f"""
        <div class="table-container" style="border-left: 4px solid {config['color']};">
            <div class="collapsible-header" onclick="toggleCollapse(this)">
                <h2 class="table-title">{config['icon']} {segment_info['description']} ({segment_info['count']} zákazníkov)</h2>
                <span class="toggle-icon">▼</span>
            </div>
            <div class="collapsible-content">
            <div style="background: #f8fafc; padding: 15px; margin-bottom: 15px; border-radius: 8px;">
                <p style="color: #1e293b; font-size: 0.9rem; margin: 0 0 8px 0;">
                    <strong>🎯 Účel:</strong> {segment_info['email_purpose']}
                </p>
                <p style="color: #1e293b; font-size: 0.9rem; margin: 0 0 8px 0;">
                    <strong>⏰ Kedy poslať:</strong> {segment_info.get('send_timing', 'Nie je definované')}
                </p>
                <p style="color: #1e293b; font-size: 0.9rem; margin: 0 0 8px 0;">
                    <strong>🏷️ Zľava:</strong> {segment_info.get('discount_suggestion', '-')}
                </p>
                <p style="color: #64748b; font-size: 0.85rem; margin: 0; font-style: italic;">
                    💬 "{segment_info.get('email_template', 'Šablóna nie je definovaná')}"
                </p>
            </div>
            <table>
                <thead>
                    <tr>"""

                for header in headers:
                    align_class = 'number' if header in ['Počet obj.', 'Celková tržba', 'Dní od posl. obj.', 'Počet pokusov'] else ''
                    html_content += f"""
                        <th class="{align_class}">{header}</th>"""

                html_content += """
                    </tr>
                </thead>
                <tbody>"""

                # Add up to 100 rows per segment (to avoid huge tables)
                for idx, (_, row) in enumerate(segment_data.head(100).iterrows()):
                    html_content += """
                    <tr>"""
                    for col in columns:
                        if col in row.index:
                            value = row[col]
                            if col == 'total_revenue':
                                html_content += f"""
                        <td class="number">€{value:,.2f}</td>"""
                            elif col in ['days_since_last_order', 'days_since_first_order', 'order_count', 'failed_order_count']:
                                html_content += f"""
                        <td class="number">{int(value) if pd.notna(value) else 'N/A'}</td>"""
                            elif col in ['last_attempt_date', 'first_order_date', 'last_order_date']:
                                date_str = pd.to_datetime(value).strftime('%Y-%m-%d') if pd.notna(value) else 'N/A'
                                html_content += f"""
                        <td>{date_str}</td>"""
                            else:
                                display_val = str(value)[:50] + '...' if len(str(value)) > 50 else str(value)
                                html_content += f"""
                        <td>{display_val if pd.notna(value) else ''}</td>"""
                        else:
                            html_content += """
                        <td></td>"""

                    html_content += """
                    </tr>"""

                # Show note if there are more rows
                if len(segment_data) > 100:
                    html_content += f"""
                    <tr class="total-row">
                        <td colspan="{len(columns)}">... a ďalších {len(segment_data) - 100} zákazníkov (celkový export v CSV súbore)</td>
                    </tr>"""

                html_content += """
                </tbody>
            </table>
            </div>
        </div>"""
            else:
                # Empty segment - show placeholder
                html_content += f"""
        <div class="table-container" style="border-left: 4px solid {config['color']}; opacity: 0.7;">
            <h2 class="table-title">{config['icon']} {segment_info['description']} (0 zákazníkov)</h2>
            <p style="color: #718096; padding: 15px;">Žiadni zákazníci v tomto segmente.</p>
        </div>"""

        # Summary card for all segments
        total_segmented = sum(s['count'] for s in customer_email_segments.values())
        html_content += f"""

        <div class="table-container" style="background: #f0fdf4; border-left: 4px solid #10B981;">
            <h2 class="table-title">Customer Segmentation Summary</h2>
            <div class="summary-cards" style="margin-top: 15px;">"""

        for segment_name, segment_info in sorted_segments:
            config = segment_configs.get(segment_name, {'color': '#6B7280', 'icon': '📋'})
            html_content += f"""
                <div class="card" style="border-left: 3px solid {config['color']};">
                    <div class="card-title">{config['icon']} {segment_name.replace('_', ' ').title()}</div>
                    <div class="card-value">{segment_info['count']}</div>
                </div>"""

        html_content += f"""
            </div>
            <p style="color: #065f46; margin-top: 15px; padding: 0 15px;">
                <strong>Poznámka:</strong> Kompletné zoznamy emailov pre každý segment sú uložené v CSV súboroch v priečinku <code>data/</code> s názvom <code>email_segment_[názov].csv</code>
            </p>
        </div>"""

    html_content += f"""

        <div class="footer">
            Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | BizniWeb Order Export System
        </div>
    </div>

    <script>
        // Collapsible table functionality
        function toggleCollapse(header) {{
            header.classList.toggle('expanded');
            const content = header.nextElementSibling;
            content.classList.toggle('expanded');
        }}

        function toggleAllTables(expand) {{
            const headers = document.querySelectorAll('.collapsible-header');
            const contents = document.querySelectorAll('.collapsible-content');
            headers.forEach(header => {{
                if (expand) {{
                    header.classList.add('expanded');
                }} else {{
                    header.classList.remove('expanded');
                }}
            }});
            contents.forEach(content => {{
                if (expand) {{
                    content.classList.add('expanded');
                }} else {{
                    content.classList.remove('expanded');
                }}
            }});
            // Update button text
            const btn = document.getElementById('toggleAllBtn');
            btn.textContent = expand ? 'Collapse All Tables' : 'Expand All Tables';
            btn.onclick = () => toggleAllTables(!expand);
        }}

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
                        label: 'Packaging Costs',
                        data: {json.dumps(packaging_costs_data)},
                        borderColor: '#38b2ac',
                        backgroundColor: 'rgba(56, 178, 172, 0.1)',
                        borderWidth: 2,
                        tension: 0.4
                    }},
                    {{
                        label: 'Shipping Subsidy',
                        data: {json.dumps(shipping_subsidy_data)},
                        borderColor: '#f97316',
                        backgroundColor: 'rgba(249, 115, 22, 0.1)',
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

        // Revenue vs Total Costs Simple Chart
        const revenueTotalCostsCtx = document.getElementById('revenueTotalCostsChart').getContext('2d');
        new Chart(revenueTotalCostsCtx, {{
            type: 'line',
            data: {{
                labels: {json.dumps(dates)},
                datasets: [
                    {{
                        label: 'Revenue',
                        data: {json.dumps(revenue_data)},
                        borderColor: '#48bb78',
                        backgroundColor: 'rgba(72, 187, 120, 0.2)',
                        borderWidth: 3,
                        tension: 0.4,
                        fill: true
                    }},
                    {{
                        label: 'Total Costs',
                        data: {json.dumps(total_costs_data)},
                        borderColor: '#f56565',
                        backgroundColor: 'rgba(245, 101, 101, 0.2)',
                        borderWidth: 3,
                        tension: 0.4,
                        fill: true
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

        // Customer Lifetime Revenue by Acquisition Date Chart
        const ltvByAcquisitionCtx = document.getElementById('ltvByAcquisitionChart').getContext('2d');
        new Chart(ltvByAcquisitionCtx, {{
            type: 'line',
            data: {{
                labels: {json.dumps(ltv_dates)},
                datasets: [
                    {{
                        label: 'Actual Daily Revenue',
                        data: {json.dumps(revenue_data)},
                        borderColor: '#63b3ed',
                        backgroundColor: 'rgba(99, 179, 237, 0.2)',
                        borderWidth: 2,
                        tension: 0.4,
                        fill: true
                    }},
                    {{
                        label: 'Full Customer Lifetime Revenue',
                        data: {json.dumps(ltv_revenue_data)},
                        borderColor: '#2b6cb0',
                        backgroundColor: 'rgba(43, 108, 176, 0.3)',
                        borderWidth: 3,
                        tension: 0.4,
                        fill: true
                    }},
                    {{
                        label: 'Total Costs',
                        data: {json.dumps(total_costs_data)},
                        borderColor: '#f56565',
                        backgroundColor: 'rgba(245, 101, 101, 0.2)',
                        borderWidth: 3,
                        tension: 0.4,
                        fill: true
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
                            }},
                            afterBody: function(context) {{
                                if (context[0].datasetIndex === 1) {{
                                    const idx = context[0].dataIndex;
                                    const actualRev = {json.dumps(revenue_data)}[idx];
                                    const ltvRev = {json.dumps(ltv_revenue_data)}[idx];
                                    if (actualRev > 0) {{
                                        const multiplier = (ltvRev / actualRev).toFixed(2);
                                        return '\\nLTV Multiplier: ' + multiplier + 'x';
                                    }}
                                }}
                                return '';
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

        // Daily Profit (LTV-Based) Chart
        const ltvProfitCtx = document.getElementById('ltvProfitChart').getContext('2d');
        new Chart(ltvProfitCtx, {{
            type: 'bar',
            data: {{
                labels: {json.dumps(ltv_dates)},
                datasets: [
                    {{
                        label: 'LTV-Based Profit',
                        data: {json.dumps(ltv_profit_data)},
                        backgroundColor: {json.dumps(ltv_profit_data)}.map(val => val >= 0 ? 'rgba(72, 187, 120, 0.6)' : 'rgba(245, 101, 101, 0.6)'),
                        borderColor: {json.dumps(ltv_profit_data)}.map(val => val >= 0 ? '#48bb78' : '#f56565'),
                        borderWidth: 2
                    }}
                ]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: true,
                aspectRatio: 2.5,
                plugins: {{
                    legend: {{
                        display: false
                    }},
                    tooltip: {{
                        callbacks: {{
                            label: function(context) {{
                                const profit = context.parsed.y;
                                return 'LTV-Based Profit: €' + profit.toFixed(2);
                            }},
                            afterBody: function(context) {{
                                const idx = context[0].dataIndex;
                                const ltvRev = {json.dumps(ltv_revenue_data)}[idx];
                                const cost = {json.dumps(total_costs_data)}[idx];
                                const actualRev = {json.dumps(revenue_data)}[idx];
                                let info = '\\nLTV Revenue: €' + ltvRev.toFixed(2);
                                info += '\\nTotal Costs: €' + cost.toFixed(2);
                                info += '\\nActual Revenue: €' + actualRev.toFixed(2);
                                if (cost > 0) {{
                                    const roi = ((ltvRev - cost) / cost * 100).toFixed(1);
                                    info += '\\nLTV ROI: ' + roi + '%';
                                }}
                                return info;
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
                        }},
                        grid: {{
                            color: function(context) {{
                                if (context.tick.value === 0) {{
                                    return 'rgba(255, 255, 255, 0.3)';
                                }}
                                return 'rgba(255, 255, 255, 0.1)';
                            }},
                            lineWidth: function(context) {{
                                if (context.tick.value === 0) {{
                                    return 2;
                                }}
                                return 1;
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
                        label: 'Shipping Subsidy',
                        data: {json.dumps(shipping_subsidy_data)},
                        borderColor: '#f97316',
                        backgroundColor: 'rgba(249, 115, 22, 0.1)',
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
                labels: ['Product Costs', 'Packaging Costs', 'Shipping Subsidy', 'Fixed Overhead', 'Facebook Ads', 'Google Ads'],
                datasets: [{{
                    data: [{total_product_expense:.2f}, {total_packaging:.2f}, {total_shipping_subsidy:.2f}, {total_fixed:.2f}, {total_fb_ads:.2f}, {total_google_ads:.2f}],
                    backgroundColor: ['#ed8936', '#f6ad55', '#f97316', '#48bb78', '#4299e1', '#34D399'],
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

        // Product Gross Margin % Chart
        const productGrossMarginCtx = document.getElementById('productGrossMarginChart').getContext('2d');
        new Chart(productGrossMarginCtx, {{
            type: 'line',
            data: {{
                labels: {json.dumps(dates)},
                datasets: [{{
                    label: 'Product Gross Margin %',
                    data: {json.dumps(product_gross_margin_daily_data)},
                    borderColor: '#22c55e',
                    backgroundColor: 'rgba(34, 197, 94, 0.15)',
                    borderWidth: 3,
                    tension: 0.35,
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
                                return 'Product Gross Margin: ' + context.parsed.y.toFixed(2) + '%';
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

        // Shipping Subsidy Chart
        const shippingSubsidyCtx = document.getElementById('shippingSubsidyChart').getContext('2d');
        new Chart(shippingSubsidyCtx, {{
            type: 'bar',
            data: {{
                labels: {json.dumps(dates)},
                datasets: [{{
                    label: 'Shipping Subsidy',
                    data: {json.dumps(shipping_subsidy_data)},
                    backgroundColor: '#f97316',
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
                                return 'Shipping Subsidy: €' + context.parsed.y.toFixed(2);
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
        }});

        // Contribution per Order Chart (Pre-Ad vs Post-Ad)
        const contributionPerOrderCtx = document.getElementById('contributionPerOrderChart').getContext('2d');
        new Chart(contributionPerOrderCtx, {{
            type: 'line',
            data: {{
                labels: {json.dumps(dates)},
                datasets: [
                    {{
                        label: 'Pre-Ad Contribution / Order',
                        data: {json.dumps(pre_ad_contribution_per_order_data)},
                        borderColor: '#10b981',
                        backgroundColor: 'rgba(16, 185, 129, 0.08)',
                        borderWidth: 2,
                        tension: 0.35,
                        fill: false
                    }},
                    {{
                        label: 'Post-Ad Contribution / Order',
                        data: {json.dumps(post_ad_contribution_per_order_data)},
                        borderColor: '#0ea5e9',
                        backgroundColor: 'rgba(14, 165, 233, 0.15)',
                        borderWidth: 3,
                        tension: 0.35,
                        fill: true
                    }}
                ]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: true,
                aspectRatio: 2.5,
                plugins: {{
                    legend: {{ display: true }},
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
                        ticks: {{
                            callback: function(value) {{
                                return '€' + value.toFixed(0);
                            }}
                        }}
                    }}
                }}
            }}
        }});

        // Average daily metrics trend chart (cumulative average through time)
        const avgDailyTrendCtx = document.getElementById('avgDailyTrendChart').getContext('2d');
        new Chart(avgDailyTrendCtx, {{
            type: 'line',
            data: {{
                labels: {json.dumps(dates)},
                datasets: [
                    {{
                        label: 'Avg Daily Revenue',
                        data: {json.dumps(cumulative_avg_revenue_data)},
                        borderColor: '#16a34a',
                        backgroundColor: 'rgba(22, 163, 74, 0.10)',
                        borderWidth: 3,
                        tension: 0.35,
                        fill: false
                    }},
                    {{
                        label: 'Avg Daily Profit/Loss',
                        data: {json.dumps(cumulative_avg_profit_data)},
                        borderColor: '#2563eb',
                        backgroundColor: 'rgba(37, 99, 235, 0.10)',
                        borderWidth: 3,
                        tension: 0.35,
                        fill: false
                    }}
                ]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: true,
                aspectRatio: 2.5,
                plugins: {{
                    legend: {{ display: true }},
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
                        ticks: {{
                            callback: function(value) {{
                                return '€' + value.toFixed(0);
                            }}
                        }}
                    }}
                }}
            }}
        }});"""

    if financial_metrics:
        break_even_cac = financial_metrics.get('break_even_cac', 0)
        paid_cac = financial_metrics.get('paid_cac', 0)
        blended_cac = financial_metrics.get('blended_cac', 0)
        html_content += f"""

        // CAC vs Break-even Comparison Chart
        const cacComparisonCtx = document.getElementById('cacComparisonChart');
        if (cacComparisonCtx) {{
            new Chart(cacComparisonCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                labels: ['Paid CAC (FB)', 'Blended CAC (Tracked Ads)', 'Break-even CAC (Customer)'],
                    datasets: [{{
                        label: 'EUR',
                        data: [{paid_cac:.2f}, {blended_cac:.2f}, {break_even_cac:.2f}],
                        backgroundColor: ['#EF4444', '#F97316', '#10B981'],
                        borderRadius: 6
                    }}]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: true,
                    aspectRatio: 2.4,
                    plugins: {{
                        legend: {{ display: false }},
                        tooltip: {{
                            callbacks: {{
                                label: function(context) {{
                                    return context.label + ': €' + context.parsed.y.toFixed(2);
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        y: {{
                            beginAtZero: true,
                            ticks: {{
                                callback: function(value) {{
                                    return '€' + value.toFixed(2);
                                }}
                            }}
                        }}
                    }}
                }}
            }});
        }}"""

    if new_vs_returning_revenue and new_vs_returning_revenue.get('daily') is not None and not new_vs_returning_revenue.get('daily').empty:
        html_content += f"""

        // New vs Returning Revenue Pie
        const newReturningRevenuePieCtx = document.getElementById('newReturningRevenuePieChart');
        if (newReturningRevenuePieCtx) {{
            new Chart(newReturningRevenuePieCtx.getContext('2d'), {{
                type: 'doughnut',
                data: {{
                    labels: ['New Customer Revenue', 'Returning Customer Revenue'],
                    datasets: [{{
                        data: [{new_ret_summary.get('new_revenue', 0):.2f}, {new_ret_summary.get('returning_revenue', 0):.2f}],
                        backgroundColor: ['#3B82F6', '#10B981'],
                        borderColor: '#fff',
                        borderWidth: 2
                    }}]
                }},
                options: {{
                    responsive: true,
                    plugins: {{
                        legend: {{ position: 'bottom' }},
                        tooltip: {{
                            callbacks: {{
                                label: function(context) {{
                                    return context.label + ': €' + context.parsed.toFixed(2);
                                }}
                            }}
                        }}
                    }}
                }}
            }});
        }}

        // New vs Returning Revenue Trend
        const newReturningRevenueTrendCtx = document.getElementById('newReturningRevenueTrendChart');
        if (newReturningRevenueTrendCtx) {{
            new Chart(newReturningRevenueTrendCtx.getContext('2d'), {{
                type: 'line',
                data: {{
                    labels: {json.dumps(new_ret_dates)},
                    datasets: [
                        {{
                            label: 'New Revenue',
                            data: {json.dumps(new_ret_new_revenue)},
                            borderColor: '#3B82F6',
                            backgroundColor: 'rgba(59, 130, 246, 0.12)',
                            borderWidth: 3,
                            tension: 0.35,
                            fill: true
                        }},
                        {{
                            label: 'Returning Revenue',
                            data: {json.dumps(new_ret_returning_revenue)},
                            borderColor: '#10B981',
                            backgroundColor: 'rgba(16, 185, 129, 0.12)',
                            borderWidth: 3,
                            tension: 0.35,
                            fill: true
                        }}
                    ]
                }},
                options: {{
                    responsive: true,
                    plugins: {{
                        legend: {{ position: 'top' }},
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
                            ticks: {{
                                callback: function(value) {{
                                    return '€' + value.toFixed(0);
                                }}
                            }}
                        }}
                    }}
                }}
            }});
        }}"""

    if refunds_analysis and refunds_analysis.get('daily') is not None and not refunds_analysis.get('daily').empty:
        html_content += f"""

        // Refund Rate Trend
        const refundRateCtx = document.getElementById('refundRateChart');
        if (refundRateCtx) {{
            new Chart(refundRateCtx.getContext('2d'), {{
                type: 'line',
                data: {{
                    labels: {json.dumps(refunds_dates)},
                    datasets: [{{
                        label: 'Refund Rate %',
                        data: {json.dumps(refunds_rate)},
                        borderColor: '#EF4444',
                        backgroundColor: 'rgba(239, 68, 68, 0.15)',
                        borderWidth: 3,
                        tension: 0.35,
                        fill: true
                    }}]
                }},
                options: {{
                    responsive: true,
                    plugins: {{
                        legend: {{ display: false }},
                        tooltip: {{
                            callbacks: {{
                                label: function(context) {{
                                    return 'Refund Rate: ' + context.parsed.y.toFixed(2) + '%';
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        y: {{
                            beginAtZero: true,
                            ticks: {{
                                callback: function(value) {{
                                    return value.toFixed(1) + '%';
                                }}
                            }}
                        }}
                    }}
                }}
            }});
        }}

        // Refund Amount Trend
        const refundAmountCtx = document.getElementById('refundAmountChart');
        if (refundAmountCtx) {{
            new Chart(refundAmountCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(refunds_dates)},
                    datasets: [{{
                        label: 'Refund Amount',
                        data: {json.dumps(refunds_amount)},
                        backgroundColor: '#F97316',
                        borderRadius: 4
                    }}]
                }},
                options: {{
                    responsive: true,
                    plugins: {{
                        legend: {{ display: false }},
                        tooltip: {{
                            callbacks: {{
                                label: function(context) {{
                                    return 'Refund Amount: €' + context.parsed.y.toFixed(2);
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
        }}"""

    # Add JavaScript for order size distribution chart if data is available
    if order_size_distribution is not None and not order_size_distribution.empty:
        # Prepare data for the chart
        size_dates = order_size_distribution['purchase_date_only'].astype(str).tolist()
        one_item = order_size_distribution['1 item'].tolist()
        two_items = order_size_distribution['2 items'].tolist()
        three_items = order_size_distribution['3 items'].tolist()
        four_items = order_size_distribution['4 items'].tolist()
        five_plus_items = order_size_distribution['5+ items'].tolist()

        html_content += f"""

        // Order Size Distribution Chart
        const orderSizeDistributionCtx = document.getElementById('orderSizeDistributionChart');
        if (orderSizeDistributionCtx) {{
            new Chart(orderSizeDistributionCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(size_dates)},
                    datasets: [
                        {{
                            label: '1 item',
                            data: {json.dumps(one_item)},
                            backgroundColor: '#3B82F6',
                            borderRadius: 3
                        }},
                        {{
                            label: '2 items',
                            data: {json.dumps(two_items)},
                            backgroundColor: '#10B981',
                            borderRadius: 3
                        }},
                        {{
                            label: '3 items',
                            data: {json.dumps(three_items)},
                            backgroundColor: '#F59E0B',
                            borderRadius: 3
                        }},
                        {{
                            label: '4 items',
                            data: {json.dumps(four_items)},
                            backgroundColor: '#EF4444',
                            borderRadius: 3
                        }},
                        {{
                            label: '5+ items',
                            data: {json.dumps(five_plus_items)},
                            backgroundColor: '#8B5CF6',
                            borderRadius: 3
                        }}
                    ]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: true,
                    aspectRatio: 2.5,
                    plugins: {{
                        legend: {{
                            position: 'top'
                        }},
                        tooltip: {{
                            mode: 'index',
                            intersect: false,
                            callbacks: {{
                                label: function(context) {{
                                    return context.dataset.label + ': ' + context.parsed.y + ' orders';
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        x: {{
                            stacked: true
                        }},
                        y: {{
                            stacked: true,
                            beginAtZero: true,
                            title: {{
                                display: true,
                                text: 'Number of Orders'
                            }},
                            ticks: {{
                                stepSize: 5
                            }}
                        }}
                    }}
                }}
            }});
        }}"""

    # Add JavaScript for Facebook Ads Analytics charts
    if fb_detailed_metrics or fb_campaigns:
        if fb_detailed_metrics:
            # Prepare data for JS
            fb_dates_js = sorted(fb_detailed_metrics.keys())
            fb_impressions_js = [fb_detailed_metrics[d].get('impressions', 0) for d in fb_dates_js]
            fb_clicks_js = [fb_detailed_metrics[d].get('clicks', 0) for d in fb_dates_js]
            fb_reach_js = [fb_detailed_metrics[d].get('reach', 0) for d in fb_dates_js]
            fb_ctr_js = [fb_detailed_metrics[d].get('ctr', 0) for d in fb_dates_js]
            fb_cpc_js = [fb_detailed_metrics[d].get('cpc', 0) for d in fb_dates_js]
            fb_cpm_js = [fb_detailed_metrics[d].get('cpm', 0) for d in fb_dates_js]
            fb_spend_js = [fb_detailed_metrics[d].get('spend', 0) for d in fb_dates_js]

            html_content += f"""

        // FB Impressions & Reach Chart
        const fbImpressionsReachCtx = document.getElementById('fbImpressionsReachChart');
        if (fbImpressionsReachCtx) {{
            new Chart(fbImpressionsReachCtx.getContext('2d'), {{
                type: 'line',
                data: {{
                    labels: {json.dumps(fb_dates_js)},
                    datasets: [
                        {{
                            label: 'Impressions',
                            data: {json.dumps(fb_impressions_js)},
                            borderColor: '#4299e1',
                            backgroundColor: 'rgba(66, 153, 225, 0.1)',
                            borderWidth: 2,
                            tension: 0.4,
                            fill: true,
                            yAxisID: 'y'
                        }},
                        {{
                            label: 'Reach',
                            data: {json.dumps(fb_reach_js)},
                            borderColor: '#48bb78',
                            backgroundColor: 'rgba(72, 187, 120, 0.1)',
                            borderWidth: 2,
                            tension: 0.4,
                            fill: true,
                            yAxisID: 'y'
                        }}
                    ]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: true,
                    aspectRatio: 2,
                    plugins: {{
                        legend: {{ position: 'top' }},
                        tooltip: {{
                            mode: 'index',
                            intersect: false,
                            callbacks: {{
                                label: function(context) {{
                                    return context.dataset.label + ': ' + context.parsed.y.toLocaleString();
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        y: {{
                            beginAtZero: true,
                            ticks: {{
                                callback: function(value) {{
                                    return value.toLocaleString();
                                }}
                            }}
                        }}
                    }}
                }}
            }});
        }}

        // FB Clicks Chart
        const fbClicksCtx = document.getElementById('fbClicksChart');
        if (fbClicksCtx) {{
            new Chart(fbClicksCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(fb_dates_js)},
                    datasets: [{{
                        label: 'Clicks',
                        data: {json.dumps(fb_clicks_js)},
                        backgroundColor: '#667eea',
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
                                    return 'Clicks: ' + context.parsed.y.toLocaleString();
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        y: {{
                            beginAtZero: true
                        }}
                    }}
                }}
            }});
        }}

        // FB CTR Chart
        const fbCtrCtx = document.getElementById('fbCtrChart');
        if (fbCtrCtx) {{
            new Chart(fbCtrCtx.getContext('2d'), {{
                type: 'line',
                data: {{
                    labels: {json.dumps(fb_dates_js)},
                    datasets: [{{
                        label: 'CTR %',
                        data: {json.dumps(fb_ctr_js)},
                        borderColor: '#9f7aea',
                        backgroundColor: 'rgba(159, 122, 234, 0.1)',
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
                                    return 'CTR: ' + context.parsed.y.toFixed(2) + '%';
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        y: {{
                            beginAtZero: true,
                            ticks: {{
                                callback: function(value) {{
                                    return value.toFixed(1) + '%';
                                }}
                            }}
                        }}
                    }}
                }}
            }});
        }}

        // FB CPC Chart
        const fbCpcCtx = document.getElementById('fbCpcChart');
        if (fbCpcCtx) {{
            new Chart(fbCpcCtx.getContext('2d'), {{
                type: 'line',
                data: {{
                    labels: {json.dumps(fb_dates_js)},
                    datasets: [{{
                        label: 'CPC',
                        data: {json.dumps(fb_cpc_js)},
                        borderColor: '#f56565',
                        backgroundColor: 'rgba(245, 101, 101, 0.1)',
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
                                    return 'CPC: €' + context.parsed.y.toFixed(2);
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        y: {{
                            beginAtZero: true,
                            ticks: {{
                                callback: function(value) {{
                                    return '€' + value.toFixed(2);
                                }}
                            }}
                        }}
                    }}
                }}
            }});
        }}

        // FB CPM Chart
        const fbCpmCtx = document.getElementById('fbCpmChart');
        if (fbCpmCtx) {{
            new Chart(fbCpmCtx.getContext('2d'), {{
                type: 'line',
                data: {{
                    labels: {json.dumps(fb_dates_js)},
                    datasets: [{{
                        label: 'CPM',
                        data: {json.dumps(fb_cpm_js)},
                        borderColor: '#ed8936',
                        backgroundColor: 'rgba(237, 137, 54, 0.1)',
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
                                    return 'CPM: €' + context.parsed.y.toFixed(2);
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        y: {{
                            beginAtZero: true,
                            ticks: {{
                                callback: function(value) {{
                                    return '€' + value.toFixed(2);
                                }}
                            }}
                        }}
                    }}
                }}
            }});
        }}

        // FB Spend vs Clicks Chart
        const fbSpendClicksCtx = document.getElementById('fbSpendClicksChart');
        if (fbSpendClicksCtx) {{
            new Chart(fbSpendClicksCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(fb_dates_js)},
                    datasets: [
                        {{
                            label: 'Spend (€)',
                            data: {json.dumps(fb_spend_js)},
                            backgroundColor: 'rgba(245, 101, 101, 0.7)',
                            borderColor: '#f56565',
                            borderWidth: 1,
                            yAxisID: 'y'
                        }},
                        {{
                            type: 'line',
                            label: 'Clicks',
                            data: {json.dumps(fb_clicks_js)},
                            borderColor: '#4299e1',
                            backgroundColor: 'transparent',
                            borderWidth: 3,
                            tension: 0.4,
                            yAxisID: 'y1'
                        }}
                    ]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: true,
                    aspectRatio: 2,
                    plugins: {{
                        legend: {{ position: 'top' }},
                        tooltip: {{
                            mode: 'index',
                            intersect: false
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
                                text: 'Spend (€)'
                            }},
                            ticks: {{
                                callback: function(value) {{
                                    return '€' + value;
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
                                text: 'Clicks'
                            }},
                            grid: {{
                                drawOnChartArea: false
                            }}
                        }}
                    }}
                }}
            }});
        }}

        // FB Efficiency Trends Chart (CPC, CPM, CTR on same chart)
        const fbEfficiencyTrendsCtx = document.getElementById('fbEfficiencyTrendsChart');
        if (fbEfficiencyTrendsCtx) {{
            new Chart(fbEfficiencyTrendsCtx.getContext('2d'), {{
                type: 'line',
                data: {{
                    labels: {json.dumps(fb_dates_js)},
                    datasets: [
                        {{
                            label: 'CPC (€)',
                            data: {json.dumps(fb_cpc_js)},
                            borderColor: '#f56565',
                            backgroundColor: 'transparent',
                            borderWidth: 2,
                            tension: 0.4,
                            yAxisID: 'y'
                        }},
                        {{
                            label: 'CPM (€)',
                            data: {json.dumps(fb_cpm_js)},
                            borderColor: '#ed8936',
                            backgroundColor: 'transparent',
                            borderWidth: 2,
                            tension: 0.4,
                            yAxisID: 'y'
                        }},
                        {{
                            label: 'CTR (%)',
                            data: {json.dumps(fb_ctr_js)},
                            borderColor: '#48bb78',
                            backgroundColor: 'transparent',
                            borderWidth: 2,
                            tension: 0.4,
                            yAxisID: 'y1'
                        }}
                    ]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: true,
                    aspectRatio: 2.5,
                    plugins: {{
                        legend: {{ position: 'top' }},
                        tooltip: {{
                            mode: 'index',
                            intersect: false,
                            callbacks: {{
                                label: function(context) {{
                                    if (context.dataset.label.includes('CTR')) {{
                                        return context.dataset.label + ': ' + context.parsed.y.toFixed(2) + '%';
                                    }}
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
                                text: 'Cost (€)'
                            }},
                            ticks: {{
                                callback: function(value) {{
                                    return '€' + value.toFixed(2);
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
                                text: 'CTR (%)'
                            }},
                            ticks: {{
                                callback: function(value) {{
                                    return value.toFixed(1) + '%';
                                }}
                            }},
                            grid: {{
                                drawOnChartArea: false
                            }}
                        }}
                    }}
                }}
            }});
        }}"""

        # Add campaign charts JavaScript
        if fb_campaigns:
            active_campaigns_js = [c for c in fb_campaigns if c.get('spend', 0) > 0]
            if active_campaigns_js:
                campaign_names = [c.get('campaign_name', 'Unknown')[:30] for c in active_campaigns_js]
                campaign_spends = [c.get('spend', 0) for c in active_campaigns_js]
                campaign_cpcs = [c.get('cpc', 0) for c in active_campaigns_js]
                campaign_ctrs = [c.get('ctr', 0) for c in active_campaigns_js]

                html_content += f"""

        // Campaign Spend Pie Chart
        const campaignSpendPieCtx = document.getElementById('campaignSpendPieChart');
        if (campaignSpendPieCtx) {{
            new Chart(campaignSpendPieCtx.getContext('2d'), {{
                type: 'doughnut',
                data: {{
                    labels: {json.dumps(campaign_names)},
                    datasets: [{{
                        data: {json.dumps(campaign_spends)},
                        backgroundColor: [
                            '#667eea', '#4299e1', '#48bb78', '#ed8936', '#f56565',
                            '#9f7aea', '#38b2ac', '#ed64a6', '#ecc94b', '#a0aec0'
                        ],
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
                            position: 'right',
                            labels: {{
                                boxWidth: 12,
                                padding: 10
                            }}
                        }},
                        tooltip: {{
                            callbacks: {{
                                label: function(context) {{
                                    const total = context.dataset.data.reduce((a, b) => a + b, 0);
                                    const pct = (context.raw / total * 100).toFixed(1);
                                    return context.label + ': €' + context.raw.toFixed(2) + ' (' + pct + '%)';
                                }}
                            }}
                        }}
                    }}
                }}
            }});
        }}

        // Campaign CPC Comparison Chart
        const campaignCpcComparisonCtx = document.getElementById('campaignCpcComparisonChart');
        if (campaignCpcComparisonCtx) {{
            new Chart(campaignCpcComparisonCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(campaign_names)},
                    datasets: [{{
                        label: 'CPC (€)',
                        data: {json.dumps(campaign_cpcs)},
                        backgroundColor: {json.dumps(campaign_cpcs)}.map(v => v < {sum(campaign_cpcs)/len(campaign_cpcs) if campaign_cpcs else 0} ? 'rgba(72, 187, 120, 0.7)' : 'rgba(245, 101, 101, 0.7)'),
                        borderColor: {json.dumps(campaign_cpcs)}.map(v => v < {sum(campaign_cpcs)/len(campaign_cpcs) if campaign_cpcs else 0} ? '#48bb78' : '#f56565'),
                        borderWidth: 1,
                        borderRadius: 5
                    }}]
                }},
                options: {{
                    indexAxis: 'y',
                    responsive: true,
                    maintainAspectRatio: true,
                    aspectRatio: 2,
                    plugins: {{
                        legend: {{ display: false }},
                        tooltip: {{
                            callbacks: {{
                                label: function(context) {{
                                    return 'CPC: €' + context.parsed.x.toFixed(2);
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        x: {{
                            beginAtZero: true,
                            ticks: {{
                                callback: function(value) {{
                                    return '€' + value.toFixed(2);
                                }}
                            }},
                            title: {{
                                display: true,
                                text: 'Cost Per Click (€) - Green = below average, Red = above average'
                            }}
                        }}
                    }}
                }}
            }});
        }}

        // Campaign CTR Comparison Chart
        const campaignCtrComparisonCtx = document.getElementById('campaignCtrComparisonChart');
        if (campaignCtrComparisonCtx) {{
            new Chart(campaignCtrComparisonCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(campaign_names)},
                    datasets: [{{
                        label: 'CTR (%)',
                        data: {json.dumps(campaign_ctrs)},
                        backgroundColor: {json.dumps(campaign_ctrs)}.map(v => v > {sum(campaign_ctrs)/len(campaign_ctrs) if campaign_ctrs else 0} ? 'rgba(72, 187, 120, 0.7)' : 'rgba(245, 101, 101, 0.7)'),
                        borderColor: {json.dumps(campaign_ctrs)}.map(v => v > {sum(campaign_ctrs)/len(campaign_ctrs) if campaign_ctrs else 0} ? '#48bb78' : '#f56565'),
                        borderWidth: 1,
                        borderRadius: 5
                    }}]
                }},
                options: {{
                    indexAxis: 'y',
                    responsive: true,
                    maintainAspectRatio: true,
                    aspectRatio: 2,
                    plugins: {{
                        legend: {{ display: false }},
                        tooltip: {{
                            callbacks: {{
                                label: function(context) {{
                                    return 'CTR: ' + context.parsed.x.toFixed(2) + '%';
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        x: {{
                            beginAtZero: true,
                            ticks: {{
                                callback: function(value) {{
                                    return value.toFixed(2) + '%';
                                }}
                            }},
                            title: {{
                                display: true,
                                text: 'Click-Through Rate (%) - Green = above average, Red = below average'
                            }}
                        }}
                    }}
                }}
            }});
        }}

        // Campaign Conversion Rate Comparison Chart
        const campaignConversionRateCtx = document.getElementById('campaignConversionRateChart');
        if (campaignConversionRateCtx) {{
            const campaignConversionRates = {json.dumps([c.get('conversion_rate', 0) for c in active_campaigns_js])};
            const avgConversionRate = campaignConversionRates.reduce((a, b) => a + b, 0) / campaignConversionRates.length;

            new Chart(campaignConversionRateCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(campaign_names)},
                    datasets: [{{
                        label: 'Conversion Rate (%)',
                        data: campaignConversionRates,
                        backgroundColor: campaignConversionRates.map(v => v > avgConversionRate ? 'rgba(72, 187, 120, 0.7)' : 'rgba(245, 101, 101, 0.7)'),
                        borderColor: campaignConversionRates.map(v => v > avgConversionRate ? '#48bb78' : '#f56565'),
                        borderWidth: 1,
                        borderRadius: 5
                    }}]
                }},
                options: {{
                    indexAxis: 'y',
                    responsive: true,
                    maintainAspectRatio: true,
                    aspectRatio: 2,
                    plugins: {{
                        legend: {{ display: false }},
                        tooltip: {{
                            callbacks: {{
                                label: function(context) {{
                                    return 'Conversion Rate: ' + context.parsed.x.toFixed(2) + '%';
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        x: {{
                            beginAtZero: true,
                            ticks: {{
                                callback: function(value) {{
                                    return value.toFixed(2) + '%';
                                }}
                            }},
                            title: {{
                                display: true,
                                text: 'Conversion Rate (%) - Green = above average, Red = below average'
                            }}
                        }}
                    }}
                }}
            }});
        }}

        // Campaign Cost Per Conversion Comparison Chart
        const campaignCostPerConversionCtx = document.getElementById('campaignCostPerConversionChart');
        if (campaignCostPerConversionCtx) {{
            const campaignCostPerConversions = {json.dumps([c.get('cost_per_conversion', 0) for c in active_campaigns_js])};
            const avgCostPerConversion = campaignCostPerConversions.filter(v => v > 0).reduce((a, b) => a + b, 0) / campaignCostPerConversions.filter(v => v > 0).length || 0;

            new Chart(campaignCostPerConversionCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(campaign_names)},
                    datasets: [{{
                        label: 'Cost per Conversion (€)',
                        data: campaignCostPerConversions,
                        backgroundColor: campaignCostPerConversions.map(v => v === 0 ? 'rgba(160, 174, 192, 0.7)' : (v < avgCostPerConversion ? 'rgba(72, 187, 120, 0.7)' : 'rgba(245, 101, 101, 0.7)')),
                        borderColor: campaignCostPerConversions.map(v => v === 0 ? '#a0aec0' : (v < avgCostPerConversion ? '#48bb78' : '#f56565')),
                        borderWidth: 1,
                        borderRadius: 5
                    }}]
                }},
                options: {{
                    indexAxis: 'y',
                    responsive: true,
                    maintainAspectRatio: true,
                    aspectRatio: 2,
                    plugins: {{
                        legend: {{ display: false }},
                        tooltip: {{
                            callbacks: {{
                                label: function(context) {{
                                    if (context.parsed.x === 0) {{
                                        return 'No conversions tracked';
                                    }}
                                    return 'Cost per Conversion: €' + context.parsed.x.toFixed(2);
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        x: {{
                            beginAtZero: true,
                            ticks: {{
                                callback: function(value) {{
                                    return '€' + value.toFixed(2);
                                }}
                            }},
                            title: {{
                                display: true,
                                text: 'Cost per Conversion (€) - Green = below average, Red = above average, Gray = no data'
                            }}
                        }}
                    }}
                }}
            }});
        }}"""

    # Add JavaScript for Cost Per Order charts
    if cost_per_order:
        weekly_cpo_js = cost_per_order.get('weekly_cpo', [])
        campaign_attribution_js = cost_per_order.get('campaign_attribution', [])
        fb_cpo_avg = cost_per_order.get('fb_cpo', 0)

        if weekly_cpo_js:
            weekly_dates = [w['week_start'] for w in weekly_cpo_js]
            weekly_cpos = [w['cpo'] for w in weekly_cpo_js]
            weekly_orders = [w['orders'] for w in weekly_cpo_js]
            weekly_spends = [w['fb_spend'] for w in weekly_cpo_js]

            html_content += f"""

        // Weekly CPO Chart
        const weeklyCpoCtx = document.getElementById('weeklyCpoChart');
        if (weeklyCpoCtx) {{
            new Chart(weeklyCpoCtx.getContext('2d'), {{
                type: 'line',
                data: {{
                    labels: {json.dumps(weekly_dates)},
                    datasets: [
                        {{
                            label: 'CPO (€)',
                            data: {json.dumps(weekly_cpos)},
                            borderColor: '#f56565',
                            backgroundColor: 'rgba(245, 101, 101, 0.1)',
                            borderWidth: 3,
                            tension: 0.4,
                            fill: true,
                            yAxisID: 'y'
                        }},
                        {{
                            label: 'Orders',
                            data: {json.dumps(weekly_orders)},
                            borderColor: '#4299e1',
                            backgroundColor: 'transparent',
                            borderWidth: 2,
                            tension: 0.4,
                            yAxisID: 'y1'
                        }}
                    ]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: true,
                    aspectRatio: 2.5,
                    plugins: {{
                        legend: {{ position: 'top' }},
                        tooltip: {{
                            mode: 'index',
                            intersect: false,
                            callbacks: {{
                                label: function(context) {{
                                    if (context.dataset.label === 'CPO (€)') {{
                                        return 'CPO: €' + context.parsed.y.toFixed(2);
                                    }}
                                    return context.dataset.label + ': ' + context.parsed.y;
                                }}
                            }}
                        }},
                        annotation: {{
                            annotations: {{
                                avgLine: {{
                                    type: 'line',
                                    yMin: {fb_cpo_avg},
                                    yMax: {fb_cpo_avg},
                                    borderColor: 'rgba(0, 0, 0, 0.5)',
                                    borderWidth: 2,
                                    borderDash: [5, 5],
                                    label: {{
                                        display: true,
                                        content: 'Avg CPO: €{fb_cpo_avg:.2f}'
                                    }}
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
                            title: {{ display: true, text: 'CPO (€)' }},
                            ticks: {{
                                callback: function(value) {{ return '€' + value.toFixed(0); }}
                            }}
                        }},
                        y1: {{
                            type: 'linear',
                            display: true,
                            position: 'right',
                            beginAtZero: true,
                            title: {{ display: true, text: 'Orders' }},
                            grid: {{ drawOnChartArea: false }}
                        }}
                    }}
                }}
            }});
        }}"""

        if campaign_attribution_js:
            camp_names_cpo = [c['campaign_name'][:25] for c in campaign_attribution_js]
            camp_cpos = [c['estimated_cpo'] for c in campaign_attribution_js]
            camp_roas = [c['estimated_roas'] for c in campaign_attribution_js]

            html_content += f"""

        // Campaign CPO Comparison Chart
        const campaignCpoCtx = document.getElementById('campaignCpoChart');
        if (campaignCpoCtx) {{
            const avgCpo = {fb_cpo_avg};
            new Chart(campaignCpoCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(camp_names_cpo)},
                    datasets: [{{
                        label: 'Est. CPO (€)',
                        data: {json.dumps(camp_cpos)},
                        backgroundColor: {json.dumps(camp_cpos)}.map(v => v < avgCpo ? 'rgba(72, 187, 120, 0.7)' : 'rgba(245, 101, 101, 0.7)'),
                        borderColor: {json.dumps(camp_cpos)}.map(v => v < avgCpo ? '#48bb78' : '#f56565'),
                        borderWidth: 1,
                        borderRadius: 5
                    }}]
                }},
                options: {{
                    indexAxis: 'y',
                    responsive: true,
                    maintainAspectRatio: true,
                    aspectRatio: 2,
                    plugins: {{
                        legend: {{ display: false }},
                        tooltip: {{
                            callbacks: {{
                                label: function(context) {{
                                    return 'Est. CPO: €' + context.parsed.x.toFixed(2);
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        x: {{
                            beginAtZero: true,
                            ticks: {{
                                callback: function(value) {{ return '€' + value.toFixed(0); }}
                            }},
                            title: {{
                                display: true,
                                text: 'Estimated Cost Per Order (€) - Green = below avg (€{fb_cpo_avg:.2f}), Red = above avg'
                            }}
                        }}
                    }}
                }}
            }});
        }}

        // Campaign ROAS Comparison Chart
        const campaignRoasCtx = document.getElementById('campaignRoasChart');
        if (campaignRoasCtx) {{
            new Chart(campaignRoasCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(camp_names_cpo)},
                    datasets: [{{
                        label: 'Est. ROAS',
                        data: {json.dumps(camp_roas)},
                        backgroundColor: {json.dumps(camp_roas)}.map(v => v >= 1 ? 'rgba(72, 187, 120, 0.7)' : 'rgba(245, 101, 101, 0.7)'),
                        borderColor: {json.dumps(camp_roas)}.map(v => v >= 1 ? '#48bb78' : '#f56565'),
                        borderWidth: 1,
                        borderRadius: 5
                    }}]
                }},
                options: {{
                    indexAxis: 'y',
                    responsive: true,
                    maintainAspectRatio: true,
                    aspectRatio: 2,
                    plugins: {{
                        legend: {{ display: false }},
                        tooltip: {{
                            callbacks: {{
                                label: function(context) {{
                                    return 'Est. ROAS: ' + context.parsed.x.toFixed(2) + 'x';
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        x: {{
                            beginAtZero: true,
                            ticks: {{
                                callback: function(value) {{ return value.toFixed(1) + 'x'; }}
                            }},
                            title: {{
                                display: true,
                                text: 'Estimated ROAS - Green = profitable (≥1x), Red = unprofitable (<1x)'
                            }}
                        }}
                    }}
                }}
            }});
        }}"""

    # Add JavaScript for Time-Based FB Ads charts
    if fb_hourly_stats:
        hourly_labels = [f"{h['hour']:02d}:00" for h in fb_hourly_stats]
        hourly_ctrs = [h.get('ctr', 0) for h in fb_hourly_stats]
        hourly_cpcs = [h.get('cpc', 0) for h in fb_hourly_stats]
        hourly_clicks = [h.get('clicks', 0) for h in fb_hourly_stats]
        hourly_spends = [h.get('spend', 0) for h in fb_hourly_stats]
        avg_ctr = sum(hourly_ctrs) / len(hourly_ctrs) if hourly_ctrs else 0
        avg_cpc = sum(hourly_cpcs) / len([c for c in hourly_cpcs if c > 0]) if any(c > 0 for c in hourly_cpcs) else 0

        html_content += f"""

        // Hourly CTR Chart
        const hourlyCtrCtx = document.getElementById('hourlyCtrChart');
        if (hourlyCtrCtx) {{
            const avgCtr = {avg_ctr};
            new Chart(hourlyCtrCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(hourly_labels)},
                    datasets: [{{
                        label: 'CTR %',
                        data: {json.dumps(hourly_ctrs)},
                        backgroundColor: {json.dumps(hourly_ctrs)}.map(v => v >= avgCtr ? 'rgba(72, 187, 120, 0.7)' : 'rgba(245, 101, 101, 0.7)'),
                        borderColor: {json.dumps(hourly_ctrs)}.map(v => v >= avgCtr ? '#48bb78' : '#f56565'),
                        borderWidth: 1,
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
                                    return 'CTR: ' + context.parsed.y.toFixed(2) + '%';
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        y: {{
                            beginAtZero: true,
                            ticks: {{
                                callback: function(value) {{ return value.toFixed(1) + '%'; }}
                            }}
                        }}
                    }}
                }}
            }});
        }}

        // Hourly CPC Chart
        const hourlyCpcCtx = document.getElementById('hourlyCpcChart');
        if (hourlyCpcCtx) {{
            const avgCpc = {avg_cpc};
            new Chart(hourlyCpcCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(hourly_labels)},
                    datasets: [{{
                        label: 'CPC €',
                        data: {json.dumps(hourly_cpcs)},
                        backgroundColor: {json.dumps(hourly_cpcs)}.map(v => v > 0 && v <= avgCpc ? 'rgba(72, 187, 120, 0.7)' : 'rgba(245, 101, 101, 0.7)'),
                        borderColor: {json.dumps(hourly_cpcs)}.map(v => v > 0 && v <= avgCpc ? '#48bb78' : '#f56565'),
                        borderWidth: 1,
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
                                    return 'CPC: €' + context.parsed.y.toFixed(2);
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        y: {{
                            beginAtZero: true,
                            ticks: {{
                                callback: function(value) {{ return '€' + value.toFixed(2); }}
                            }}
                        }}
                    }}
                }}
            }});
        }}

        // Hourly Clicks Chart
        const hourlyClicksCtx = document.getElementById('hourlyClicksChart');
        if (hourlyClicksCtx) {{
            new Chart(hourlyClicksCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(hourly_labels)},
                    datasets: [{{
                        label: 'Clicks',
                        data: {json.dumps(hourly_clicks)},
                        backgroundColor: '#667eea',
                        borderRadius: 5
                    }}]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: true,
                    aspectRatio: 2,
                    plugins: {{
                        legend: {{ display: false }}
                    }},
                    scales: {{
                        y: {{ beginAtZero: true }}
                    }}
                }}
            }});
        }}

        // Hourly Spend Chart
        const hourlySpendCtx = document.getElementById('hourlySpendChart');
        if (hourlySpendCtx) {{
            new Chart(hourlySpendCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(hourly_labels)},
                    datasets: [{{
                        label: 'Spend €',
                        data: {json.dumps(hourly_spends)},
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
                                    return 'Spend: €' + context.parsed.y.toFixed(2);
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        y: {{
                            beginAtZero: true,
                            ticks: {{
                                callback: function(value) {{ return '€' + value; }}
                            }}
                        }}
                    }}
                }}
            }});
        }}

        // Hourly Efficiency Chart (Spend vs CTR)
        const hourlyEfficiencyCtx = document.getElementById('hourlyEfficiencyChart');
        if (hourlyEfficiencyCtx) {{
            new Chart(hourlyEfficiencyCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(hourly_labels)},
                    datasets: [
                        {{
                            label: 'Spend €',
                            data: {json.dumps(hourly_spends)},
                            backgroundColor: 'rgba(66, 153, 225, 0.7)',
                            borderColor: '#4299e1',
                            borderWidth: 1,
                            yAxisID: 'y'
                        }},
                        {{
                            type: 'line',
                            label: 'CTR %',
                            data: {json.dumps(hourly_ctrs)},
                            borderColor: '#48bb78',
                            backgroundColor: 'transparent',
                            borderWidth: 3,
                            tension: 0.4,
                            yAxisID: 'y1'
                        }}
                    ]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: true,
                    aspectRatio: 2.5,
                    plugins: {{
                        legend: {{ position: 'top' }},
                        tooltip: {{ mode: 'index', intersect: false }}
                    }},
                    scales: {{
                        y: {{
                            type: 'linear',
                            display: true,
                            position: 'left',
                            beginAtZero: true,
                            title: {{ display: true, text: 'Spend (€)' }},
                            ticks: {{ callback: function(value) {{ return '€' + value; }} }}
                        }},
                        y1: {{
                            type: 'linear',
                            display: true,
                            position: 'right',
                            beginAtZero: true,
                            title: {{ display: true, text: 'CTR (%)' }},
                            ticks: {{ callback: function(value) {{ return value.toFixed(1) + '%'; }} }},
                            grid: {{ drawOnChartArea: false }}
                        }}
                    }}
                }}
            }});
        }}"""

    # Add JavaScript for Hourly CPO charts
    if fb_hourly_stats and cost_per_order:
        hourly_orders_data = cost_per_order.get('hourly_orders', [])
        if hourly_orders_data:
            # Build hourly CPO data for JS
            orders_by_hour_js = {h['hour']: h for h in hourly_orders_data}
            hourly_cpo_js = []
            hourly_orders_js = []
            hourly_revenue_js = []
            hourly_roas_js = []

            for fb_hour in fb_hourly_stats:
                hour = fb_hour['hour']
                spend = fb_hour.get('spend', 0)
                order_data = orders_by_hour_js.get(hour, {'orders': 0, 'revenue': 0})
                orders = order_data.get('orders', 0)
                revenue = order_data.get('revenue', 0)

                cpo = spend / orders if orders > 0 else 0
                roas = revenue / spend if spend > 0 else 0

                hourly_cpo_js.append(round(cpo, 2))
                hourly_orders_js.append(orders)
                hourly_revenue_js.append(round(revenue, 2))
                hourly_roas_js.append(round(roas, 2))

            avg_hourly_cpo_js = sum(c for c in hourly_cpo_js if c > 0) / len([c for c in hourly_cpo_js if c > 0]) if any(c > 0 for c in hourly_cpo_js) else 0

            html_content += f"""

        // Hourly CPO Chart
        const hourlyCpoCtx = document.getElementById('hourlyCpoChart');
        if (hourlyCpoCtx) {{
            const avgCpo = {avg_hourly_cpo_js};
            new Chart(hourlyCpoCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(hourly_labels)},
                    datasets: [{{
                        label: 'CPO €',
                        data: {json.dumps(hourly_cpo_js)},
                        backgroundColor: {json.dumps(hourly_cpo_js)}.map(v => v > 0 && v <= avgCpo ? 'rgba(72, 187, 120, 0.7)' : 'rgba(245, 101, 101, 0.7)'),
                        borderColor: {json.dumps(hourly_cpo_js)}.map(v => v > 0 && v <= avgCpo ? '#48bb78' : '#f56565'),
                        borderWidth: 1,
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
                                    return 'CPO: €' + context.parsed.y.toFixed(2);
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        y: {{
                            beginAtZero: true,
                            ticks: {{ callback: function(value) {{ return '€' + value.toFixed(0); }} }}
                        }}
                    }}
                }}
            }});
        }}

        // Hourly Orders Chart
        const hourlyOrdersCtx = document.getElementById('hourlyOrdersChart');
        if (hourlyOrdersCtx) {{
            new Chart(hourlyOrdersCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(hourly_labels)},
                    datasets: [{{
                        label: 'Orders',
                        data: {json.dumps(hourly_orders_js)},
                        backgroundColor: '#667eea',
                        borderRadius: 5
                    }}]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: true,
                    aspectRatio: 2,
                    plugins: {{
                        legend: {{ display: false }}
                    }},
                    scales: {{
                        y: {{ beginAtZero: true }}
                    }}
                }}
            }});
        }}

        // Hourly ROAS Chart
        const hourlyRoasCtx = document.getElementById('hourlyRoasChart');
        if (hourlyRoasCtx) {{
            new Chart(hourlyRoasCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(hourly_labels)},
                    datasets: [{{
                        label: 'ROAS',
                        data: {json.dumps(hourly_roas_js)},
                        backgroundColor: {json.dumps(hourly_roas_js)}.map(v => v >= 1 ? 'rgba(72, 187, 120, 0.7)' : 'rgba(245, 101, 101, 0.7)'),
                        borderColor: {json.dumps(hourly_roas_js)}.map(v => v >= 1 ? '#48bb78' : '#f56565'),
                        borderWidth: 1,
                        borderRadius: 5
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
                                    return 'ROAS: ' + context.parsed.y.toFixed(2) + 'x';
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        y: {{
                            beginAtZero: true,
                            ticks: {{ callback: function(value) {{ return value.toFixed(1) + 'x'; }} }}
                        }}
                    }}
                }}
            }});
        }}

        // Hourly Spend vs Orders vs CPO Chart
        const hourlySpendOrdersCpoCtx = document.getElementById('hourlySpendOrdersCpoChart');
        if (hourlySpendOrdersCpoCtx) {{
            new Chart(hourlySpendOrdersCpoCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(hourly_labels)},
                    datasets: [
                        {{
                            label: 'Spend €',
                            data: {json.dumps(hourly_spends)},
                            backgroundColor: 'rgba(237, 137, 54, 0.7)',
                            borderColor: '#ed8936',
                            borderWidth: 1,
                            yAxisID: 'y'
                        }},
                        {{
                            type: 'line',
                            label: 'Orders',
                            data: {json.dumps(hourly_orders_js)},
                            borderColor: '#4299e1',
                            backgroundColor: 'transparent',
                            borderWidth: 3,
                            tension: 0.4,
                            yAxisID: 'y1'
                        }},
                        {{
                            type: 'line',
                            label: 'CPO €',
                            data: {json.dumps(hourly_cpo_js)},
                            borderColor: '#f56565',
                            backgroundColor: 'transparent',
                            borderWidth: 3,
                            borderDash: [5, 5],
                            tension: 0.4,
                            yAxisID: 'y2'
                        }}
                    ]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: true,
                    aspectRatio: 2.5,
                    plugins: {{
                        legend: {{ position: 'top' }},
                        tooltip: {{
                            mode: 'index',
                            intersect: false,
                            callbacks: {{
                                label: function(context) {{
                                    if (context.dataset.label === 'Spend €' || context.dataset.label === 'CPO €') {{
                                        return context.dataset.label + ': €' + context.parsed.y.toFixed(2);
                                    }}
                                    return context.dataset.label + ': ' + context.parsed.y;
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
                            title: {{ display: true, text: 'Spend (€)' }},
                            ticks: {{ callback: function(value) {{ return '€' + value; }} }}
                        }},
                        y1: {{
                            type: 'linear',
                            display: true,
                            position: 'right',
                            beginAtZero: true,
                            title: {{ display: true, text: 'Orders' }},
                            grid: {{ drawOnChartArea: false }}
                        }},
                        y2: {{
                            type: 'linear',
                            display: false,
                            beginAtZero: true
                        }}
                    }}
                }}
            }});
        }}"""

    # Add JavaScript for Day of Week charts
    if fb_dow_stats:
        dow_sorted = sorted(fb_dow_stats, key=lambda x: x.get('day_num', 0))
        dow_labels = [d.get('day_of_week', '')[:3] for d in dow_sorted]
        dow_ctrs = [d.get('ctr', 0) for d in dow_sorted]
        dow_cpcs = [d.get('cpc', 0) for d in dow_sorted]
        dow_spends = [d.get('total_spend', 0) for d in dow_sorted]
        dow_clicks = [d.get('total_clicks', 0) for d in dow_sorted]
        avg_dow_ctr = sum(dow_ctrs) / len(dow_ctrs) if dow_ctrs else 0
        avg_dow_cpc = sum([c for c in dow_cpcs if c > 0]) / len([c for c in dow_cpcs if c > 0]) if any(c > 0 for c in dow_cpcs) else 0

        html_content += f"""

        // Day of Week CTR Chart
        const dowCtrCtx = document.getElementById('dowCtrChart');
        if (dowCtrCtx) {{
            const avgCtr = {avg_dow_ctr};
            new Chart(dowCtrCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(dow_labels)},
                    datasets: [{{
                        label: 'CTR %',
                        data: {json.dumps(dow_ctrs)},
                        backgroundColor: {json.dumps(dow_ctrs)}.map(v => v >= avgCtr ? 'rgba(72, 187, 120, 0.7)' : 'rgba(245, 101, 101, 0.7)'),
                        borderColor: {json.dumps(dow_ctrs)}.map(v => v >= avgCtr ? '#48bb78' : '#f56565'),
                        borderWidth: 1,
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
                                    return 'CTR: ' + context.parsed.y.toFixed(2) + '%';
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        y: {{
                            beginAtZero: true,
                            ticks: {{ callback: function(value) {{ return value.toFixed(1) + '%'; }} }}
                        }}
                    }}
                }}
            }});
        }}

        // Day of Week CPC Chart
        const dowCpcCtx = document.getElementById('dowCpcChart');
        if (dowCpcCtx) {{
            const avgCpc = {avg_dow_cpc};
            new Chart(dowCpcCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(dow_labels)},
                    datasets: [{{
                        label: 'CPC €',
                        data: {json.dumps(dow_cpcs)},
                        backgroundColor: {json.dumps(dow_cpcs)}.map(v => v > 0 && v <= avgCpc ? 'rgba(72, 187, 120, 0.7)' : 'rgba(245, 101, 101, 0.7)'),
                        borderColor: {json.dumps(dow_cpcs)}.map(v => v > 0 && v <= avgCpc ? '#48bb78' : '#f56565'),
                        borderWidth: 1,
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
                                    return 'CPC: €' + context.parsed.y.toFixed(2);
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        y: {{
                            beginAtZero: true,
                            ticks: {{ callback: function(value) {{ return '€' + value.toFixed(2); }} }}
                        }}
                    }}
                }}
            }});
        }}

        // Day of Week Spend vs Clicks Chart
        const dowSpendClicksCtx = document.getElementById('dowSpendClicksChart');
        if (dowSpendClicksCtx) {{
            new Chart(dowSpendClicksCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(dow_labels)},
                    datasets: [
                        {{
                            label: 'Total Spend €',
                            data: {json.dumps(dow_spends)},
                            backgroundColor: 'rgba(245, 101, 101, 0.7)',
                            borderColor: '#f56565',
                            borderWidth: 1,
                            yAxisID: 'y'
                        }},
                        {{
                            type: 'line',
                            label: 'Total Clicks',
                            data: {json.dumps(dow_clicks)},
                            borderColor: '#4299e1',
                            backgroundColor: 'transparent',
                            borderWidth: 3,
                            tension: 0.4,
                            yAxisID: 'y1'
                        }}
                    ]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: true,
                    aspectRatio: 2.5,
                    plugins: {{
                        legend: {{ position: 'top' }},
                        tooltip: {{ mode: 'index', intersect: false }}
                    }},
                    scales: {{
                        y: {{
                            type: 'linear',
                            display: true,
                            position: 'left',
                            beginAtZero: true,
                            title: {{ display: true, text: 'Spend (€)' }},
                            ticks: {{ callback: function(value) {{ return '€' + value; }} }}
                        }},
                        y1: {{
                            type: 'linear',
                            display: true,
                            position: 'right',
                            beginAtZero: true,
                            title: {{ display: true, text: 'Clicks' }},
                            grid: {{ drawOnChartArea: false }}
                        }}
                    }}
                }}
            }});
        }}"""

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
        }}

        // New vs Returning Customer Orders Trend Chart
        const newVsReturningCtx = document.getElementById('newVsReturningTrendChart');
        if (newVsReturningCtx) {{
            new Chart(newVsReturningCtx.getContext('2d'), {{
                type: 'line',
                data: {{
                    labels: {json.dumps(week_starts)},
                    datasets: [
                        {{
                            label: 'New Customer Orders',
                            data: {json.dumps(new_orders)},
                            borderColor: '#10B981',
                            backgroundColor: 'rgba(16, 185, 129, 0.1)',
                            borderWidth: 3,
                            tension: 0.4,
                            fill: true,
                            pointRadius: 5,
                            pointBackgroundColor: '#10B981'
                        }},
                        {{
                            label: 'Returning Customer Orders',
                            data: {json.dumps(returning_orders)},
                            borderColor: '#3B82F6',
                            backgroundColor: 'rgba(59, 130, 246, 0.1)',
                            borderWidth: 3,
                            tension: 0.4,
                            fill: true,
                            pointRadius: 5,
                            pointBackgroundColor: '#3B82F6'
                        }}
                    ]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: true,
                    aspectRatio: 2.5,
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
                        y: {{
                            beginAtZero: true,
                            title: {{ display: true, text: 'Number of Orders' }}
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
                        }},
                        {{
                            label: 'Cumulative Avg CAC (€)',
                            data: {json.dumps(cumulative_cac)},
                            borderColor: '#667eea',
                            backgroundColor: 'rgba(102, 126, 234, 0.1)',
                            borderWidth: 3,
                            tension: 0.4,
                            borderDash: [5, 5]
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
        
        // Revenue LTV/CAC Ratio Chart
        const ltvCacRatioCtx = document.getElementById('ltvCacRatioChart');
        if (ltvCacRatioCtx) {{
            new Chart(ltvCacRatioCtx.getContext('2d'), {{
                type: 'line',
                data: {{
                    labels: {json.dumps(clv_week_starts)},
                    datasets: [
                        {{
                            label: 'Revenue LTV/CAC',
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
                                text: 'Revenue LTV/CAC'
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

        // Estimated Payback Period Chart (in orders)
        const paybackCtx = document.getElementById('paybackChart');
        if (paybackCtx) {{
            new Chart(paybackCtx.getContext('2d'), {{
                type: 'line',
                data: {{
                    labels: {json.dumps(payback_weekly_labels)},
                    datasets: [
                        {{
                            label: 'Estimated Payback (Orders)',
                            data: {json.dumps(payback_weekly_orders)},
                            borderColor: '#0ea5e9',
                            backgroundColor: 'rgba(14, 165, 233, 0.12)',
                            borderWidth: 3,
                            tension: 0.35,
                            fill: true
                        }},
                        {{
                            label: 'Break-even (1.0)',
                            data: Array({len(payback_weekly_labels)}).fill(1),
                            borderColor: '#718096',
                            borderWidth: 2,
                            borderDash: [8, 6],
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
                                    if (context.dataset.label.includes('Break-even')) {{
                                        return context.dataset.label;
                                    }}
                                    return context.dataset.label + ': ' + context.parsed.y.toFixed(2) + ' orders';
                                }},
                                afterBody: function() {{
                                    return 'Pre-ad contribution/order: €{pre_ad_contribution_per_order:.2f}';
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        y: {{
                            beginAtZero: true,
                            title: {{
                                display: true,
                                text: 'Orders Needed to Recover CAC'
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

    # Add JavaScript for item combinations chart if data is available
    if item_combinations is not None and not item_combinations.empty:
        # Prepare data for the chart - top 20 combinations sorted by count
        top_combos = item_combinations.nlargest(20, 'count')
        # Shorten combination names for display (convert newlines to " + " for chart)
        combo_labels = []
        combo_full_labels = []  # Full labels for tooltips
        for combo in top_combos['combination'].tolist():
            # Store full label for tooltip (with newlines converted to line separator)
            combo_full = combo.replace('\n', ' + ')
            combo_full_labels.append(combo_full)
            # Truncate for Y-axis display
            if len(combo_full) > 50:
                combo_labels.append(combo_full[:47] + '...')
            else:
                combo_labels.append(combo_full)
        combo_counts = top_combos['count'].tolist()
        combo_sizes = top_combos['combination_size'].tolist()

        # Color based on combination size
        colors = []
        for size in combo_sizes:
            if size == 2:
                colors.append('#3B82F6')  # Blue
            elif size == 3:
                colors.append('#10B981')  # Green
            elif size == 4:
                colors.append('#F59E0B')  # Yellow
            else:
                colors.append('#EF4444')  # Red

        html_content += f"""

        // Item Combinations Chart - store full labels for tooltips
        const comboFullLabels = {json.dumps(combo_full_labels)};
        const itemCombinationsCtx = document.getElementById('itemCombinationsChart');
        if (itemCombinationsCtx) {{
            new Chart(itemCombinationsCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(combo_labels)},
                    datasets: [{{
                        label: 'Times Ordered Together',
                        data: {json.dumps(combo_counts)},
                        backgroundColor: {json.dumps(colors)},
                        borderRadius: 5
                    }}]
                }},
                options: {{
                    indexAxis: 'y',
                    responsive: true,
                    maintainAspectRatio: true,
                    aspectRatio: 1.5,
                    plugins: {{
                        legend: {{
                            display: false
                        }},
                        tooltip: {{
                            callbacks: {{
                                title: function(context) {{
                                    // Show full combination title on hover
                                    return comboFullLabels[context[0].dataIndex];
                                }},
                                label: function(context) {{
                                    return 'Ordered together: ' + context.parsed.x + ' times';
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        x: {{
                            beginAtZero: true,
                            title: {{
                                display: true,
                                text: 'Number of Orders'
                            }},
                            ticks: {{
                                stepSize: 1
                            }}
                        }},
                        y: {{
                            ticks: {{
                                font: {{
                                    size: 10
                                }}
                            }}
                        }}
                    }}
                }}
            }});
        }}"""

    # Day of Week Charts
    if day_of_week_analysis is not None and not day_of_week_analysis.empty:
        dow_labels = day_of_week_analysis['day_name'].tolist()
        dow_orders = day_of_week_analysis['orders'].tolist()
        dow_revenue = day_of_week_analysis['revenue'].tolist()
        dow_fb_spend = day_of_week_analysis['fb_spend'].tolist() if 'fb_spend' in day_of_week_analysis.columns else [0] * len(dow_labels)

        html_content += f"""

        // Day of Week Orders Chart with FB Spend
        const dowOrdersCtx = document.getElementById('dowOrdersChart');
        if (dowOrdersCtx) {{
            new Chart(dowOrdersCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(dow_labels)},
                    datasets: [{{
                        label: 'Orders',
                        data: {json.dumps(dow_orders)},
                        backgroundColor: '#3B82F6',
                        borderRadius: 5,
                        yAxisID: 'y'
                    }}, {{
                        label: 'FB Spend',
                        data: {json.dumps(dow_fb_spend)},
                        type: 'line',
                        borderColor: '#F59E0B',
                        backgroundColor: 'rgba(245, 158, 11, 0.1)',
                        borderWidth: 3,
                        pointRadius: 5,
                        pointBackgroundColor: '#F59E0B',
                        fill: true,
                        yAxisID: 'y1'
                    }}]
                }},
                options: {{
                    responsive: true,
                    plugins: {{ legend: {{ display: true, position: 'top' }} }},
                    scales: {{
                        y: {{
                            type: 'linear',
                            position: 'left',
                            beginAtZero: true,
                            title: {{ display: true, text: 'Orders' }}
                        }},
                        y1: {{
                            type: 'linear',
                            position: 'right',
                            beginAtZero: true,
                            title: {{ display: true, text: 'FB Spend (€)' }},
                            ticks: {{ callback: function(v) {{ return '€' + v.toLocaleString(); }} }},
                            grid: {{ drawOnChartArea: false }}
                        }}
                    }}
                }}
            }});
        }}

        // Day of Week Revenue Chart with FB Spend
        const dowRevenueCtx = document.getElementById('dowRevenueChart');
        if (dowRevenueCtx) {{
            new Chart(dowRevenueCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(dow_labels)},
                    datasets: [{{
                        label: 'Revenue',
                        data: {json.dumps(dow_revenue)},
                        backgroundColor: '#10B981',
                        borderRadius: 5,
                        yAxisID: 'y'
                    }}, {{
                        label: 'FB Spend',
                        data: {json.dumps(dow_fb_spend)},
                        type: 'line',
                        borderColor: '#F59E0B',
                        backgroundColor: 'rgba(245, 158, 11, 0.1)',
                        borderWidth: 3,
                        pointRadius: 5,
                        pointBackgroundColor: '#F59E0B',
                        fill: true,
                        yAxisID: 'y1'
                    }}]
                }},
                options: {{
                    responsive: true,
                    plugins: {{ legend: {{ display: true, position: 'top' }} }},
                    scales: {{
                        y: {{
                            type: 'linear',
                            position: 'left',
                            beginAtZero: true,
                            title: {{ display: true, text: 'Revenue (€)' }},
                            ticks: {{ callback: function(v) {{ return '€' + v.toLocaleString(); }} }}
                        }},
                        y1: {{
                            type: 'linear',
                            position: 'right',
                            beginAtZero: true,
                            title: {{ display: true, text: 'FB Spend (€)' }},
                            ticks: {{ callback: function(v) {{ return '€' + v.toLocaleString(); }} }},
                            grid: {{ drawOnChartArea: false }}
                        }}
                    }}
                }}
            }});
        }}"""

    # Country Chart
    if country_analysis is not None and not country_analysis.empty:
        country_labels = country_analysis['country'].tolist()[:10]
        country_revenue = country_analysis['revenue'].tolist()[:10]

        html_content += f"""

        // Country Revenue Chart
        const countryCtx = document.getElementById('countryChart');
        if (countryCtx) {{
            new Chart(countryCtx.getContext('2d'), {{
                type: 'doughnut',
                data: {{
                    labels: {json.dumps(country_labels)},
                    datasets: [{{
                        data: {json.dumps(country_revenue)},
                        backgroundColor: ['#3B82F6', '#10B981', '#F59E0B', '#EF4444', '#8B5CF6', '#EC4899', '#06B6D4', '#84CC16', '#F97316', '#6366F1']
                    }}]
                }},
                options: {{
                    responsive: true,
                    plugins: {{
                        legend: {{ position: 'right' }},
                        tooltip: {{ callbacks: {{ label: function(ctx) {{ return ctx.label + ': €' + ctx.raw.toLocaleString(); }} }} }}
                    }}
                }}
            }});
        }}"""

    if geo_profitability and isinstance(geo_profitability, dict):
        geo_table = geo_profitability.get('table')
        if geo_table is not None and not geo_table.empty:
            geo_labels = [str(c).upper() for c in geo_table['country'].tolist()]
            geo_margin = geo_table['contribution_margin_pct'].tolist()
            geo_cpo = geo_table['fb_cpo'].tolist()
            html_content += f"""

        // Geo Profitability Chart
        const geoProfitabilityCtx = document.getElementById('geoProfitabilityChart');
        if (geoProfitabilityCtx) {{
            new Chart(geoProfitabilityCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(geo_labels)},
                    datasets: [
                        {{
                            type: 'bar',
                            label: 'Contribution Margin %',
                            data: {json.dumps(geo_margin)},
                            backgroundColor: '#10B981',
                            borderRadius: 5,
                            yAxisID: 'y'
                        }},
                        {{
                            type: 'line',
                            label: 'FB CPO (€)',
                            data: {json.dumps(geo_cpo)},
                            borderColor: '#EF4444',
                            backgroundColor: 'rgba(239, 68, 68, 0.1)',
                            borderWidth: 3,
                            tension: 0.35,
                            yAxisID: 'y1'
                        }}
                    ]
                }},
                options: {{
                    responsive: true,
                    plugins: {{
                        legend: {{ position: 'top' }},
                        tooltip: {{
                            callbacks: {{
                                label: function(context) {{
                                    if (context.dataset.label.includes('Margin')) {{
                                        return context.dataset.label + ': ' + context.parsed.y.toFixed(2) + '%';
                                    }}
                                    return context.dataset.label + ': €' + context.parsed.y.toFixed(2);
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        y: {{
                            type: 'linear',
                            position: 'left',
                            title: {{ display: true, text: 'Contribution Margin %' }},
                            ticks: {{ callback: function(v) {{ return v.toFixed(0) + '%'; }} }}
                        }},
                        y1: {{
                            type: 'linear',
                            position: 'right',
                            title: {{ display: true, text: 'FB CPO (€)' }},
                            grid: {{ drawOnChartArea: false }},
                            ticks: {{ callback: function(v) {{ return '€' + v.toFixed(2); }} }}
                        }}
                    }}
                }}
            }});
        }}"""

    # B2B Chart
    if b2b_analysis is not None and not b2b_analysis.empty:
        b2b_labels = b2b_analysis['customer_type'].tolist()
        b2b_revenue = b2b_analysis['revenue'].tolist()
        b2b_orders = b2b_analysis['orders'].tolist()

        html_content += f"""

        // B2B vs B2C Chart
        const b2bCtx = document.getElementById('b2bChart');
        if (b2bCtx) {{
            new Chart(b2bCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(b2b_labels)},
                    datasets: [
                        {{ label: 'Revenue (€)', data: {json.dumps(b2b_revenue)}, backgroundColor: '#3B82F6', yAxisID: 'y' }},
                        {{ label: 'Orders', data: {json.dumps(b2b_orders)}, backgroundColor: '#10B981', yAxisID: 'y1' }}
                    ]
                }},
                options: {{
                    responsive: true,
                    scales: {{
                        y: {{ type: 'linear', position: 'left', beginAtZero: true }},
                        y1: {{ type: 'linear', position: 'right', beginAtZero: true, grid: {{ drawOnChartArea: false }} }}
                    }}
                }}
            }});
        }}"""

    # Customer Concentration Chart - with all levels (10%, 20%, 30%, 40%, 50%, remaining)
    if customer_concentration:
        level_revenue_share = customer_concentration.get('level_revenue_share', {})
        top_10_pct = level_revenue_share.get(10, customer_concentration.get('top_10_pct_revenue_share', 0))
        top_20_pct = level_revenue_share.get(20, customer_concentration.get('top_20_pct_revenue_share', 0))
        top_30_pct = level_revenue_share.get(30, top_20_pct)
        top_40_pct = level_revenue_share.get(40, top_30_pct)
        top_50_pct = level_revenue_share.get(50, top_40_pct)

        # Calculate incremental values for stacked display
        pct_10 = top_10_pct
        pct_20 = top_20_pct - top_10_pct
        pct_30 = top_30_pct - top_20_pct
        pct_40 = top_40_pct - top_30_pct
        pct_50 = top_50_pct - top_40_pct
        pct_remaining = 100 - top_50_pct

        html_content += f"""

        // Customer Concentration Chart - expanded levels
        const concCtx = document.getElementById('customerConcentrationChart');
        if (concCtx) {{
            new Chart(concCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: ['Top 10%', 'Top 11-20%', 'Top 21-30%', 'Top 31-40%', 'Top 41-50%', 'Remaining 50%'],
                    datasets: [{{
                        label: 'Revenue Share %',
                        data: [{pct_10}, {pct_20}, {pct_30}, {pct_40}, {pct_50}, {pct_remaining}],
                        backgroundColor: ['#EF4444', '#F97316', '#F59E0B', '#EAB308', '#84CC16', '#10B981'],
                        borderRadius: 5
                    }}]
                }},
                options: {{
                    responsive: true,
                    plugins: {{
                        legend: {{ display: false }},
                        tooltip: {{
                            callbacks: {{
                                label: function(context) {{
                                    return context.parsed.y.toFixed(1) + '% of total revenue';
                                }}
                            }}
                        }}
                    }},
                    scales: {{ y: {{ beginAtZero: true, max: 100, title: {{ display: true, text: 'Revenue Share %' }} }} }}
                }}
            }});
        }}"""

    # Product Margins Chart
    if product_margins is not None and not product_margins.empty:
        top_margin_products = product_margins.head(20)
        margin_labels = [p[:30] + '...' if len(p) > 30 else p for p in top_margin_products['product'].tolist()]
        margin_values = top_margin_products['margin_pct'].tolist()
        margin_colors = ['#10B981' if m > 0 else '#EF4444' for m in margin_values]

        html_content += f"""

        // Product Margins Chart
        const marginCtx = document.getElementById('marginChart');
        if (marginCtx) {{
            new Chart(marginCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(margin_labels)},
                    datasets: [{{
                        label: 'Margin %',
                        data: {json.dumps(margin_values)},
                        backgroundColor: {json.dumps(margin_colors)},
                        borderRadius: 5
                    }}]
                }},
                options: {{
                    indexAxis: 'y',
                    responsive: true,
                    plugins: {{ legend: {{ display: false }} }},
                    scales: {{ x: {{ beginAtZero: true }} }}
                }}
            }});
        }}"""

    # Ads Correlation Charts with trend lines and ROI-based colors
    # Use date_agg for accurate ROI calculation (includes ALL costs: product, packaging, fixed, ads)
    if ads_effectiveness and date_agg is not None and not date_agg.empty:
        daily_data = ads_effectiveness.get('daily_data')
        if daily_data is not None and not daily_data.empty:
            # Filter data where fb_spend > 0
            valid_data = daily_data[daily_data['fb_spend'] > 0].copy()

            # Merge with date_agg to get accurate net_profit (includes all costs)
            valid_data['date'] = pd.to_datetime(valid_data['date']).dt.strftime('%Y-%m-%d')
            date_agg_lookup = date_agg.copy()
            date_agg_lookup['date'] = pd.to_datetime(date_agg_lookup['date']).dt.strftime('%Y-%m-%d')
            valid_data = valid_data.merge(
                date_agg_lookup[['date', 'net_profit', 'roi_percent', 'total_cost']],
                on='date',
                how='left'
            )

            # Prepare data for both charts
            orders_scatter_data = []
            revenue_scatter_data = []
            ads_point_colors = []
            ads_profit_values = []

            for _, row in valid_data.iterrows():
                fb_spend = row.get('fb_spend', 0)
                orders = row.get('orders', 0)
                revenue = row.get('revenue', 0)
                # Use net_profit from date_agg (includes ALL costs: product, packaging, fixed, ads)
                net_profit = row.get('net_profit', 0)
                if pd.isna(net_profit):
                    net_profit = 0

                orders_scatter_data.append({'x': fb_spend, 'y': orders})
                revenue_scatter_data.append({'x': fb_spend, 'y': revenue})
                ads_profit_values.append(round(net_profit, 2))
                # Green for positive profit (after ALL costs), Red for negative profit
                ads_point_colors.append('#10B981' if net_profit >= 0 else '#EF4444')

            # Calculate linear regression for Orders trend line
            orders_trend_data = []
            revenue_trend_data = []
            if len(valid_data) >= 2:
                x_values = valid_data['fb_spend'].values
                min_x = min(x_values)
                max_x = max(x_values)
                n = len(x_values)
                sum_x = sum(x_values)
                sum_x2 = sum(x ** 2 for x in x_values)

                # Orders trend line
                y_orders = valid_data['orders'].values
                sum_y_orders = sum(y_orders)
                sum_xy_orders = sum(x * y for x, y in zip(x_values, y_orders))
                if n * sum_x2 - sum_x ** 2 != 0:
                    slope_orders = (n * sum_xy_orders - sum_x * sum_y_orders) / (n * sum_x2 - sum_x ** 2)
                    intercept_orders = (sum_y_orders - slope_orders * sum_x) / n
                else:
                    slope_orders = 0
                    intercept_orders = sum_y_orders / n if n > 0 else 0
                orders_trend_data = [
                    {'x': float(min_x), 'y': float(slope_orders * min_x + intercept_orders)},
                    {'x': float(max_x), 'y': float(slope_orders * max_x + intercept_orders)}
                ]

                # Revenue trend line
                y_revenue = valid_data['revenue'].values
                sum_y_revenue = sum(y_revenue)
                sum_xy_revenue = sum(x * y for x, y in zip(x_values, y_revenue))
                if n * sum_x2 - sum_x ** 2 != 0:
                    slope_revenue = (n * sum_xy_revenue - sum_x * sum_y_revenue) / (n * sum_x2 - sum_x ** 2)
                    intercept_revenue = (sum_y_revenue - slope_revenue * sum_x) / n
                else:
                    slope_revenue = 0
                    intercept_revenue = sum_y_revenue / n if n > 0 else 0
                revenue_trend_data = [
                    {'x': float(min_x), 'y': float(slope_revenue * min_x + intercept_revenue)},
                    {'x': float(max_x), 'y': float(slope_revenue * max_x + intercept_revenue)}
                ]

            html_content += f"""

        // Ad Spend vs Orders Chart (Green = profit, Red = loss)
        const adsOrdersCtx = document.getElementById('adsOrdersChart');
        const adsProfitValues = {json.dumps(ads_profit_values)};
        if (adsOrdersCtx) {{
            new Chart(adsOrdersCtx.getContext('2d'), {{
                type: 'scatter',
                data: {{
                    datasets: [{{
                        label: 'FB Spend vs Orders',
                        data: {json.dumps(orders_scatter_data)},
                        backgroundColor: {json.dumps(ads_point_colors)},
                        pointRadius: 8,
                        pointHoverRadius: 10
                    }}, {{
                        label: 'Trend Line',
                        data: {json.dumps(orders_trend_data)},
                        type: 'line',
                        borderColor: '#6366F1',
                        borderWidth: 2,
                        borderDash: [5, 5],
                        pointRadius: 0,
                        fill: false
                    }}]
                }},
                options: {{
                    responsive: true,
                    plugins: {{
                        legend: {{
                            display: true,
                            position: 'top',
                            labels: {{
                                generateLabels: function(chart) {{
                                    return [
                                        {{ text: 'Profitable Day', fillStyle: '#10B981', strokeStyle: '#10B981' }},
                                        {{ text: 'Loss Day', fillStyle: '#EF4444', strokeStyle: '#EF4444' }},
                                        {{ text: 'Trend Line', fillStyle: 'transparent', strokeStyle: '#6366F1', lineDash: [5, 5] }}
                                    ];
                                }}
                            }}
                        }},
                        tooltip: {{
                            callbacks: {{
                                label: function(context) {{
                                    if (context.dataset.label === 'Trend Line') {{
                                        return 'Predicted: ' + context.parsed.y.toFixed(1) + ' orders';
                                    }}
                                    var profit = adsProfitValues[context.dataIndex];
                                    return ['Spend: €' + context.parsed.x.toFixed(2), 'Orders: ' + context.parsed.y, 'Profit: €' + profit.toFixed(2)];
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        x: {{ title: {{ display: true, text: 'FB Spend (€)' }} }},
                        y: {{ title: {{ display: true, text: 'Orders' }}, beginAtZero: true }}
                    }}
                }}
            }});
        }}

        // Ad Spend vs Revenue Chart (Green = profit, Red = loss)
        const adsRevenueCtx = document.getElementById('adsRevenueChart');
        if (adsRevenueCtx) {{
            new Chart(adsRevenueCtx.getContext('2d'), {{
                type: 'scatter',
                data: {{
                    datasets: [{{
                        label: 'FB Spend vs Revenue',
                        data: {json.dumps(revenue_scatter_data)},
                        backgroundColor: {json.dumps(ads_point_colors)},
                        pointRadius: 8,
                        pointHoverRadius: 10
                    }}, {{
                        label: 'Trend Line',
                        data: {json.dumps(revenue_trend_data)},
                        type: 'line',
                        borderColor: '#6366F1',
                        borderWidth: 2,
                        borderDash: [5, 5],
                        pointRadius: 0,
                        fill: false
                    }}]
                }},
                options: {{
                    responsive: true,
                    plugins: {{
                        legend: {{
                            display: true,
                            position: 'top',
                            labels: {{
                                generateLabels: function(chart) {{
                                    return [
                                        {{ text: 'Profitable Day', fillStyle: '#10B981', strokeStyle: '#10B981' }},
                                        {{ text: 'Loss Day', fillStyle: '#EF4444', strokeStyle: '#EF4444' }},
                                        {{ text: 'Trend Line', fillStyle: 'transparent', strokeStyle: '#6366F1', lineDash: [5, 5] }}
                                    ];
                                }}
                            }}
                        }},
                        tooltip: {{
                            callbacks: {{
                                label: function(context) {{
                                    if (context.dataset.label === 'Trend Line') {{
                                        return 'Predicted: €' + context.parsed.y.toFixed(2);
                                    }}
                                    var profit = adsProfitValues[context.dataIndex];
                                    return ['Spend: €' + context.parsed.x.toFixed(2), 'Revenue: €' + context.parsed.y.toFixed(2), 'Profit: €' + profit.toFixed(2)];
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        x: {{ title: {{ display: true, text: 'FB Spend (€)' }} }},
                        y: {{ title: {{ display: true, text: 'Revenue (€)' }}, beginAtZero: true }}
                    }}
                }}
            }});
        }}"""

    # Cost vs Revenue Correlation Chart with ROI-based colors
    if date_agg is not None and not date_agg.empty and 'total_cost' in date_agg.columns:
        cost_revenue_data = []
        point_colors = []
        roi_values = []

        for _, row in date_agg.iterrows():
            if row['total_cost'] > 0:
                cost_revenue_data.append({'x': row['total_cost'], 'y': row['total_revenue']})
                roi = row.get('roi_percent', 0)
                roi_values.append(roi)
                # Green for positive ROI, Red for negative ROI
                point_colors.append('#10B981' if roi >= 0 else '#EF4444')

        # Calculate correlation
        if len(date_agg) >= 2:
            cost_values = date_agg['total_cost'].values
            revenue_values = date_agg['total_revenue'].values
            correlation = date_agg['total_cost'].corr(date_agg['total_revenue'])
            correlation = round(correlation, 3) if not pd.isna(correlation) else 0

            # Calculate linear regression for trend line
            n = len(cost_values)
            sum_x = sum(cost_values)
            sum_y = sum(revenue_values)
            sum_xy = sum(x * y for x, y in zip(cost_values, revenue_values))
            sum_x2 = sum(x ** 2 for x in cost_values)

            if n * sum_x2 - sum_x ** 2 != 0:
                slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x ** 2)
                intercept = (sum_y - slope * sum_x) / n
            else:
                slope = 0
                intercept = sum_y / n if n > 0 else 0

            min_cost = min(cost_values)
            max_cost = max(cost_values)
            cost_trend_data = [
                {'x': float(min_cost), 'y': float(slope * min_cost + intercept)},
                {'x': float(max_cost), 'y': float(slope * max_cost + intercept)}
            ]
        else:
            correlation = 0
            cost_trend_data = []

        html_content += f"""

        // Cost vs Revenue Correlation Chart (Green = positive ROI, Red = negative ROI)
        const costRevenueCtx = document.getElementById('costRevenueChart');
        const roiValues = {json.dumps(roi_values)};
        if (costRevenueCtx) {{
            new Chart(costRevenueCtx.getContext('2d'), {{
                type: 'scatter',
                data: {{
                    datasets: [{{
                        label: 'Cost vs Revenue (Corr: {correlation})',
                        data: {json.dumps(cost_revenue_data)},
                        backgroundColor: {json.dumps(point_colors)},
                        pointRadius: 8,
                        pointHoverRadius: 10
                    }}, {{
                        label: 'Trend Line',
                        data: {json.dumps(cost_trend_data)},
                        type: 'line',
                        borderColor: '#6366F1',
                        borderWidth: 2,
                        borderDash: [5, 5],
                        pointRadius: 0,
                        fill: false
                    }}]
                }},
                options: {{
                    responsive: true,
                    plugins: {{
                        legend: {{
                            display: true,
                            position: 'top',
                            labels: {{
                                generateLabels: function(chart) {{
                                    return [
                                        {{ text: 'Positive ROI', fillStyle: '#10B981', strokeStyle: '#10B981' }},
                                        {{ text: 'Negative ROI', fillStyle: '#EF4444', strokeStyle: '#EF4444' }},
                                        {{ text: 'Trend Line', fillStyle: 'transparent', strokeStyle: '#6366F1', lineDash: [5, 5] }}
                                    ];
                                }}
                            }}
                        }},
                        tooltip: {{
                            callbacks: {{
                                label: function(context) {{
                                    if (context.dataset.label === 'Trend Line') {{
                                        return 'Predicted Revenue: €' + context.parsed.y.toFixed(2);
                                    }}
                                    var roi = roiValues[context.dataIndex];
                                    return ['Cost: €' + context.parsed.x.toFixed(2), 'Revenue: €' + context.parsed.y.toFixed(2), 'ROI: ' + roi.toFixed(1) + '%'];
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        x: {{ title: {{ display: true, text: 'Total Cost (€)' }} }},
                        y: {{ title: {{ display: true, text: 'Revenue (€)' }}, beginAtZero: true }}
                    }}
                }}
            }});
        }}"""

    # Spend Range Effectiveness Charts (Orders and Revenue)
    if ads_effectiveness:
        spend_effectiveness = ads_effectiveness.get('spend_effectiveness')
        if spend_effectiveness is not None and not spend_effectiveness.empty:
            range_labels = spend_effectiveness['spend_range'].astype(str).tolist()
            range_orders = spend_effectiveness['avg_orders'].tolist()
            range_revenue = spend_effectiveness['avg_revenue'].tolist()
            range_roas = spend_effectiveness['roas'].tolist()
            range_spend = spend_effectiveness['avg_spend'].tolist()

            html_content += f"""

        // FB Spend Range - Orders Chart
        const spendRangeOrdersCtx = document.getElementById('spendRangeOrdersChart');
        if (spendRangeOrdersCtx) {{
            new Chart(spendRangeOrdersCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(range_labels)},
                    datasets: [{{
                        label: 'Avg Orders',
                        data: {json.dumps(range_orders)},
                        backgroundColor: '#3B82F6',
                        borderRadius: 5
                    }}]
                }},
                options: {{
                    responsive: true,
                    plugins: {{
                        legend: {{ display: false }},
                        tooltip: {{
                            callbacks: {{
                                afterLabel: function(context) {{
                                    var idx = context.dataIndex;
                                    var spendValues = {json.dumps(range_spend)};
                                    return 'Avg Spend: €' + spendValues[idx].toFixed(2);
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        y: {{
                            beginAtZero: true,
                            title: {{ display: true, text: 'Average Orders' }}
                        }}
                    }}
                }}
            }});
        }}

        // FB Spend Range - Revenue Chart
        const spendRangeRevenueCtx = document.getElementById('spendRangeRevenueChart');
        if (spendRangeRevenueCtx) {{
            new Chart(spendRangeRevenueCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(range_labels)},
                    datasets: [{{
                        label: 'Avg Revenue',
                        data: {json.dumps(range_revenue)},
                        backgroundColor: '#10B981',
                        borderRadius: 5,
                        yAxisID: 'y'
                    }}, {{
                        label: 'ROAS (x)',
                        data: {json.dumps(range_roas)},
                        type: 'line',
                        borderColor: '#F59E0B',
                        backgroundColor: 'rgba(245, 158, 11, 0.1)',
                        borderWidth: 3,
                        pointRadius: 5,
                        pointBackgroundColor: '#F59E0B',
                        fill: false,
                        yAxisID: 'y1'
                    }}]
                }},
                options: {{
                    responsive: true,
                    plugins: {{
                        legend: {{ position: 'top' }},
                        tooltip: {{
                            callbacks: {{
                                afterLabel: function(context) {{
                                    var idx = context.dataIndex;
                                    var spendValues = {json.dumps(range_spend)};
                                    return 'Avg Spend: €' + spendValues[idx].toFixed(2);
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        y: {{
                            type: 'linear',
                            position: 'left',
                            beginAtZero: true,
                            title: {{ display: true, text: 'Avg Revenue (€)' }},
                            ticks: {{ callback: function(v) {{ return '€' + v.toLocaleString(); }} }}
                        }},
                        y1: {{
                            type: 'linear',
                            position: 'right',
                            beginAtZero: true,
                            title: {{ display: true, text: 'ROAS (x)' }},
                            grid: {{ drawOnChartArea: false }}
                        }}
                    }}
                }}
            }});
        }}"""

    # Order Status Chart
    if order_status is not None and not order_status.empty:
        status_labels = order_status['status'].tolist()
        status_orders = order_status['orders'].tolist()

        html_content += f"""

        // Order Status Chart
        const statusCtx = document.getElementById('statusChart');
        if (statusCtx) {{
            new Chart(statusCtx.getContext('2d'), {{
                type: 'pie',
                data: {{
                    labels: {json.dumps(status_labels)},
                    datasets: [{{
                        data: {json.dumps(status_orders)},
                        backgroundColor: ['#10B981', '#3B82F6', '#F59E0B', '#EF4444', '#8B5CF6', '#EC4899', '#06B6D4']
                    }}]
                }},
                options: {{
                    responsive: true,
                    plugins: {{ legend: {{ position: 'right' }} }}
                }}
            }});
        }}"""

    # Add Cohort Analysis Charts
    if cohort_analysis is not None:
        order_freq = cohort_analysis.get('order_frequency', pd.DataFrame())
        time_between = cohort_analysis.get('time_between_orders', pd.DataFrame())
        time_between_by_order = cohort_analysis.get('time_between_by_order_num', pd.DataFrame())
        time_to_nth = cohort_analysis.get('time_to_nth_order', pd.DataFrame())
        revenue_by_order = cohort_analysis.get('revenue_by_order_num', pd.DataFrame())
        cohort_retention = cohort_analysis.get('cohort_retention', pd.DataFrame())

        # Order Frequency Chart
        if not order_freq.empty:
            freq_labels = order_freq['frequency'].tolist()
            freq_customers = order_freq['customer_count'].tolist()
            freq_orders = order_freq['total_orders'].tolist()

            html_content += f"""

        // Order Frequency Distribution Chart
        const orderFreqCtx = document.getElementById('orderFrequencyChart');
        if (orderFreqCtx) {{
            new Chart(orderFreqCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(freq_labels)},
                    datasets: [
                        {{
                            label: 'Customers',
                            data: {json.dumps(freq_customers)},
                            backgroundColor: '#3B82F6',
                            borderRadius: 5,
                            yAxisID: 'y'
                        }},
                        {{
                            label: 'Orders',
                            data: {json.dumps(freq_orders)},
                            backgroundColor: '#10B981',
                            borderRadius: 5,
                            yAxisID: 'y1'
                        }}
                    ]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: true,
                    aspectRatio: 2,
                    plugins: {{
                        legend: {{ position: 'top' }},
                        tooltip: {{
                            mode: 'index',
                            intersect: false
                        }}
                    }},
                    scales: {{
                        y: {{
                            type: 'linear',
                            position: 'left',
                            beginAtZero: true,
                            title: {{ display: true, text: 'Customers' }}
                        }},
                        y1: {{
                            type: 'linear',
                            position: 'right',
                            beginAtZero: true,
                            title: {{ display: true, text: 'Orders' }},
                            grid: {{ drawOnChartArea: false }}
                        }}
                    }}
                }}
            }});
        }}"""

        # Time Between Orders Chart
        if not time_between.empty:
            time_labels = time_between['time_bucket'].tolist()
            time_counts = time_between['count'].tolist()
            time_pcts = time_between['percentage'].tolist()

            html_content += f"""

        // Time Between Orders Chart
        const timeBetweenCtx = document.getElementById('timeBetweenOrdersChart');
        if (timeBetweenCtx) {{
            new Chart(timeBetweenCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(time_labels)},
                    datasets: [{{
                        label: 'Orders',
                        data: {json.dumps(time_counts)},
                        backgroundColor: '#8B5CF6',
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
                                    const pcts = {json.dumps(time_pcts)};
                                    return context.parsed.y + ' orders (' + pcts[context.dataIndex] + '%)';
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        y: {{
                            beginAtZero: true,
                            title: {{ display: true, text: 'Number of Repeat Orders' }}
                        }}
                    }}
                }}
            }});
        }}"""

        # Time Between Orders by Order Number Chart
        if not time_between_by_order.empty:
            transition_labels = time_between_by_order['transition'].tolist()
            transition_avg_days = time_between_by_order['avg_days'].tolist()
            transition_median_days = time_between_by_order['median_days'].tolist()
            transition_counts = time_between_by_order['count'].tolist()

            html_content += f"""

        // Time Between Orders by Order Number Chart
        const timeBetweenByOrderCtx = document.getElementById('timeBetweenByOrderChart');
        if (timeBetweenByOrderCtx) {{
            new Chart(timeBetweenByOrderCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(transition_labels)},
                    datasets: [
                        {{
                            label: 'Avg Days Between',
                            data: {json.dumps(transition_avg_days)},
                            backgroundColor: '#8B5CF6',
                            borderRadius: 5,
                            yAxisID: 'y'
                        }},
                        {{
                            label: 'Median Days Between',
                            data: {json.dumps(transition_median_days)},
                            backgroundColor: '#EC4899',
                            borderRadius: 5,
                            yAxisID: 'y'
                        }},
                        {{
                            type: 'line',
                            label: 'Number of Customers',
                            data: {json.dumps(transition_counts)},
                            borderColor: '#10B981',
                            backgroundColor: 'rgba(16, 185, 129, 0.1)',
                            borderWidth: 3,
                            tension: 0.4,
                            pointRadius: 6,
                            pointBackgroundColor: '#10B981',
                            yAxisID: 'y1'
                        }}
                    ]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: true,
                    aspectRatio: 2.5,
                    plugins: {{
                        legend: {{ position: 'top' }},
                        tooltip: {{
                            mode: 'index',
                            intersect: false,
                            callbacks: {{
                                label: function(context) {{
                                    if (context.dataset.label === 'Number of Customers') {{
                                        return context.dataset.label + ': ' + context.parsed.y;
                                    }}
                                    return context.dataset.label + ': ' + context.parsed.y + ' days';
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        y: {{
                            type: 'linear',
                            position: 'left',
                            beginAtZero: true,
                            title: {{ display: true, text: 'Days Between Orders' }}
                        }},
                        y1: {{
                            type: 'linear',
                            position: 'right',
                            beginAtZero: true,
                            title: {{ display: true, text: 'Number of Customers' }},
                            grid: {{ drawOnChartArea: false }}
                        }}
                    }}
                }}
            }});
        }}"""

        # Time to Nth Order Chart
        if not time_to_nth.empty:
            nth_labels = time_to_nth['order_number'].tolist()
            nth_avg_days = time_to_nth['avg_days_from_first'].tolist()
            nth_median_days = time_to_nth['median_days_from_first'].tolist()
            nth_customers = time_to_nth['customer_count'].tolist()

            html_content += f"""

        // Time to Nth Order Chart
        const timeToNthCtx = document.getElementById('timeToNthOrderChart');
        if (timeToNthCtx) {{
            new Chart(timeToNthCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(nth_labels)},
                    datasets: [
                        {{
                            label: 'Average Days',
                            data: {json.dumps(nth_avg_days)},
                            backgroundColor: '#3B82F6',
                            borderRadius: 5
                        }},
                        {{
                            label: 'Median Days',
                            data: {json.dumps(nth_median_days)},
                            backgroundColor: '#10B981',
                            borderRadius: 5
                        }}
                    ]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: true,
                    aspectRatio: 2,
                    plugins: {{
                        legend: {{ position: 'top' }},
                        tooltip: {{
                            callbacks: {{
                                afterBody: function(context) {{
                                    const customers = {json.dumps(nth_customers)};
                                    return 'Customers: ' + customers[context[0].dataIndex];
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        y: {{
                            beginAtZero: true,
                            title: {{ display: true, text: 'Days from First Order' }}
                        }}
                    }}
                }}
            }});
        }}"""

        # AOV by Order Number Chart (enhanced with items and price per item)
        if not revenue_by_order.empty:
            aov_labels = [f'Order #{int(x)}' for x in revenue_by_order['order_number'].tolist()]
            aov_values = revenue_by_order['avg_order_value'].tolist()
            aov_counts = revenue_by_order['order_count'].tolist()
            avg_items = revenue_by_order['avg_items_per_order'].tolist() if 'avg_items_per_order' in revenue_by_order.columns else [0] * len(aov_labels)
            avg_price_per_item = revenue_by_order['avg_price_per_item'].tolist() if 'avg_price_per_item' in revenue_by_order.columns else [0] * len(aov_labels)

            html_content += f"""

        // Order Metrics by Order Sequence Chart
        const aovByOrderCtx = document.getElementById('aovByOrderNumChart');
        if (aovByOrderCtx) {{
            new Chart(aovByOrderCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(aov_labels)},
                    datasets: [
                        {{
                            type: 'bar',
                            label: 'Avg Items per Order',
                            data: {json.dumps(avg_items)},
                            backgroundColor: 'rgba(16, 185, 129, 0.7)',
                            borderColor: '#10B981',
                            borderWidth: 1,
                            borderRadius: 5,
                            yAxisID: 'y1'
                        }},
                        {{
                            type: 'line',
                            label: 'Avg Order Value (€)',
                            data: {json.dumps(aov_values)},
                            borderColor: '#F59E0B',
                            backgroundColor: 'rgba(245, 158, 11, 0.1)',
                            borderWidth: 3,
                            tension: 0.4,
                            fill: false,
                            pointRadius: 6,
                            pointBackgroundColor: '#F59E0B',
                            yAxisID: 'y'
                        }},
                        {{
                            type: 'line',
                            label: 'Avg Price per Item (€)',
                            data: {json.dumps(avg_price_per_item)},
                            borderColor: '#8B5CF6',
                            backgroundColor: 'rgba(139, 92, 246, 0.1)',
                            borderWidth: 3,
                            borderDash: [5, 5],
                            tension: 0.4,
                            fill: false,
                            pointRadius: 6,
                            pointBackgroundColor: '#8B5CF6',
                            yAxisID: 'y'
                        }}
                    ]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: true,
                    aspectRatio: 2,
                    plugins: {{
                        legend: {{ position: 'top' }},
                        tooltip: {{
                            mode: 'index',
                            intersect: false,
                            callbacks: {{
                                label: function(context) {{
                                    if (context.dataset.label.includes('€')) {{
                                        return context.dataset.label + ': €' + context.parsed.y.toFixed(2);
                                    }}
                                    return context.dataset.label + ': ' + context.parsed.y.toFixed(2);
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        y: {{
                            type: 'linear',
                            position: 'left',
                            beginAtZero: true,
                            title: {{ display: true, text: 'Value (€)' }},
                            ticks: {{
                                callback: function(value) {{
                                    return '€' + value.toFixed(0);
                                }}
                            }}
                        }},
                        y1: {{
                            type: 'linear',
                            position: 'right',
                            beginAtZero: true,
                            title: {{ display: true, text: 'Avg Items per Order' }},
                            grid: {{ drawOnChartArea: false }}
                        }}
                    }}
                }}
            }});
        }}"""

        # Cohort Retention Chart
        if not cohort_retention.empty:
            cohort_labels = cohort_retention['cohort'].tolist()
            retention_2nd = cohort_retention['retention_2nd_pct'].tolist()
            retention_3rd = cohort_retention['retention_3rd_pct'].tolist()
            retention_4th = cohort_retention['retention_4th_pct'].tolist()
            retention_5th = cohort_retention['retention_5th_pct'].tolist()

            html_content += f"""

        // Cohort Retention Chart
        const cohortRetentionCtx = document.getElementById('cohortRetentionChart');
        if (cohortRetentionCtx) {{
            new Chart(cohortRetentionCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(cohort_labels)},
                    datasets: [
                        {{
                            label: '2nd Order %',
                            data: {json.dumps(retention_2nd)},
                            backgroundColor: '#3B82F6',
                            borderRadius: 3
                        }},
                        {{
                            label: '3rd Order %',
                            data: {json.dumps(retention_3rd)},
                            backgroundColor: '#10B981',
                            borderRadius: 3
                        }},
                        {{
                            label: '4th Order %',
                            data: {json.dumps(retention_4th)},
                            backgroundColor: '#F59E0B',
                            borderRadius: 3
                        }},
                        {{
                            label: '5th Order %',
                            data: {json.dumps(retention_5th)},
                            backgroundColor: '#EF4444',
                            borderRadius: 3
                        }}
                    ]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: true,
                    aspectRatio: 2.5,
                    plugins: {{
                        legend: {{ position: 'top' }},
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
                            title: {{ display: true, text: 'Retention Rate (%)' }},
                            ticks: {{
                                callback: function(value) {{
                                    return value + '%';
                                }}
                            }}
                        }}
                    }}
                }}
            }});
        }}"""

        # Mature Cohort Retention Chart (time-bias-free)
        mature_cohorts = cohort_analysis.get('mature_cohort_retention', pd.DataFrame())
        if not mature_cohorts.empty:
            mature_labels = mature_cohorts['cohort'].tolist()
            mature_2nd = mature_cohorts['retention_2nd_pct'].tolist()
            mature_3rd = mature_cohorts['retention_3rd_pct'].tolist()
            mature_4th = mature_cohorts['retention_4th_pct'].tolist()
            mature_5th = mature_cohorts['retention_5th_pct'].tolist()
            mature_customers = mature_cohorts['total_customers'].tolist()

            html_content += f"""

        // Mature Cohort Retention Chart (time-bias-free)
        const matureCohortCtx = document.getElementById('matureCohortRetentionChart');
        if (matureCohortCtx) {{
            new Chart(matureCohortCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(mature_labels)},
                    datasets: [
                        {{
                            label: '2nd Order %',
                            data: {json.dumps(mature_2nd)},
                            backgroundColor: '#059669',
                            borderRadius: 3
                        }},
                        {{
                            label: '3rd Order %',
                            data: {json.dumps(mature_3rd)},
                            backgroundColor: '#10B981',
                            borderRadius: 3
                        }},
                        {{
                            label: '4th Order %',
                            data: {json.dumps(mature_4th)},
                            backgroundColor: '#34D399',
                            borderRadius: 3
                        }},
                        {{
                            label: '5th Order %',
                            data: {json.dumps(mature_5th)},
                            backgroundColor: '#6EE7B7',
                            borderRadius: 3
                        }}
                    ]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: true,
                    aspectRatio: 2.5,
                    plugins: {{
                        legend: {{ position: 'top' }},
                        tooltip: {{
                            mode: 'index',
                            intersect: false,
                            callbacks: {{
                                label: function(context) {{
                                    return context.dataset.label + ': ' + context.parsed.y.toFixed(1) + '%';
                                }},
                                afterBody: function(context) {{
                                    const customers = {json.dumps(mature_customers)};
                                    return 'Customers in cohort: ' + customers[context[0].dataIndex];
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        y: {{
                            beginAtZero: true,
                            max: 50,
                            title: {{ display: true, text: 'Retention Rate (%)' }},
                            ticks: {{
                                callback: function(value) {{
                                    return value + '%';
                                }}
                            }}
                        }}
                    }}
                }}
            }});
        }}"""

    # Add First Item Retention Chart
    if first_item_retention is not None:
        item_retention_df = first_item_retention.get('item_retention', pd.DataFrame())
        if not item_retention_df.empty:
            first_item_labels = [name[:30] + '...' if len(str(name)) > 30 else name for name in item_retention_df['item_name'].tolist()]
            first_item_2nd = item_retention_df['retention_2nd_pct'].tolist()
            first_item_3rd = item_retention_df['retention_3rd_pct'].tolist()
            first_item_customers = item_retention_df['first_order_customers'].tolist()

            html_content += f"""

        // First Item Retention Chart
        const firstItemRetentionCtx = document.getElementById('firstItemRetentionChart');
        if (firstItemRetentionCtx) {{
            new Chart(firstItemRetentionCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(first_item_labels)},
                    datasets: [
                        {{
                            label: '2nd Order %',
                            data: {json.dumps(first_item_2nd)},
                            backgroundColor: '#667eea',
                            borderRadius: 3
                        }},
                        {{
                            label: '3rd Order %',
                            data: {json.dumps(first_item_3rd)},
                            backgroundColor: '#a78bfa',
                            borderRadius: 3
                        }}
                    ]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: true,
                    aspectRatio: 2.5,
                    indexAxis: 'y',
                    plugins: {{
                        legend: {{ position: 'top' }},
                        tooltip: {{
                            mode: 'index',
                            intersect: false,
                            callbacks: {{
                                label: function(context) {{
                                    return context.dataset.label + ': ' + context.parsed.x.toFixed(1) + '%';
                                }},
                                afterBody: function(context) {{
                                    const customers = {json.dumps(first_item_customers)};
                                    return 'First order customers: ' + customers[context[0].dataIndex];
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        x: {{
                            beginAtZero: true,
                            max: 50,
                            title: {{ display: true, text: 'Retention Rate (%)' }},
                            ticks: {{
                                callback: function(value) {{
                                    return value + '%';
                                }}
                            }}
                        }}
                    }}
                }}
            }});
        }}"""

    # Add Time to Nth by First Item Chart
    if time_to_nth_by_first_item is not None:
        time_to_nth_df = time_to_nth_by_first_item.get('time_to_nth_by_item', pd.DataFrame())
        if not time_to_nth_df.empty:
            time_item_labels = [name[:30] + '...' if len(str(name)) > 30 else name for name in time_to_nth_df['item_name'].tolist()]
            time_to_2nd = [v if pd.notna(v) else 0 for v in time_to_nth_df.get('avg_days_to_2nd', pd.Series([0]*len(time_to_nth_df))).tolist()]
            time_customers = time_to_nth_df['first_order_customers'].tolist()

            html_content += f"""

        // Time to Nth by First Item Chart
        const timeToNthByFirstItemCtx = document.getElementById('timeToNthByFirstItemChart');
        if (timeToNthByFirstItemCtx) {{
            new Chart(timeToNthByFirstItemCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(time_item_labels)},
                    datasets: [
                        {{
                            label: 'Avg Days to 2nd Order',
                            data: {json.dumps(time_to_2nd)},
                            backgroundColor: '#f59e0b',
                            borderRadius: 3
                        }}
                    ]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: true,
                    aspectRatio: 2.5,
                    indexAxis: 'y',
                    plugins: {{
                        legend: {{ position: 'top' }},
                        tooltip: {{
                            mode: 'index',
                            intersect: false,
                            callbacks: {{
                                label: function(context) {{
                                    return context.dataset.label + ': ' + context.parsed.x.toFixed(1) + ' days';
                                }},
                                afterBody: function(context) {{
                                    const customers = {json.dumps(time_customers)};
                                    return 'First order customers: ' + customers[context[0].dataIndex];
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        x: {{
                            beginAtZero: true,
                            title: {{ display: true, text: 'Days to 2nd Order' }}
                        }}
                    }}
                }}
            }});
        }}"""

    # Add Same Item Repurchase Chart
    if same_item_repurchase is not None:
        item_repurchase_df = same_item_repurchase.get('item_repurchase', pd.DataFrame())
        if not item_repurchase_df.empty:
            repurchase_labels = [name[:30] + '...' if len(str(name)) > 30 else name for name in item_repurchase_df['item_name'].tolist()]
            repurchase_2x = item_repurchase_df['repurchase_2x_pct'].tolist()
            repurchase_3x = item_repurchase_df['repurchase_3x_pct'].tolist()
            repurchase_customers = item_repurchase_df['unique_customers'].tolist()

            html_content += f"""

        // Same Item Repurchase Chart
        const sameItemRepurchaseCtx = document.getElementById('sameItemRepurchaseChart');
        if (sameItemRepurchaseCtx) {{
            new Chart(sameItemRepurchaseCtx.getContext('2d'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(repurchase_labels)},
                    datasets: [
                        {{
                            label: '2x+ Repurchase %',
                            data: {json.dumps(repurchase_2x)},
                            backgroundColor: '#10B981',
                            borderRadius: 3
                        }},
                        {{
                            label: '3x+ Repurchase %',
                            data: {json.dumps(repurchase_3x)},
                            backgroundColor: '#34D399',
                            borderRadius: 3
                        }}
                    ]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: true,
                    aspectRatio: 2.5,
                    indexAxis: 'y',
                    plugins: {{
                        legend: {{ position: 'top' }},
                        tooltip: {{
                            mode: 'index',
                            intersect: false,
                            callbacks: {{
                                label: function(context) {{
                                    return context.dataset.label + ': ' + context.parsed.x.toFixed(1) + '%';
                                }},
                                afterBody: function(context) {{
                                    const customers = {json.dumps(repurchase_customers)};
                                    return 'Unique customers: ' + customers[context[0].dataIndex];
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        x: {{
                            beginAtZero: true,
                            max: 60,
                            title: {{ display: true, text: 'Repurchase Rate (%)' }},
                            ticks: {{
                                callback: function(value) {{
                                    return value + '%';
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


def generate_email_strategy_report(customer_email_segments: dict, cohort_analysis: dict,
                                    date_from: datetime, date_to: datetime) -> str:
    """
    Generate a separate HTML report with complete email marketing strategy in Slovak
    including email templates and customer lists for each segment.
    """

    summary = cohort_analysis.get('summary', {}) if cohort_analysis else {}

    html_content = f"""<!DOCTYPE html>
<html lang="sk">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Email Marketing Stratégia - Vevo | {date_from.strftime('%Y-%m-%d')} až {date_to.strftime('%Y-%m-%d')}</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); min-height: 100vh; padding: 20px; }}
        .container {{ max-width: 1400px; margin: 0 auto; }}
        .header {{ background: white; border-radius: 20px; padding: 40px; margin-bottom: 30px; text-align: center; box-shadow: 0 10px 40px rgba(0,0,0,0.1); }}
        .header h1 {{ color: #1a1a2e; font-size: 2.5rem; margin-bottom: 10px; }}
        .header p {{ color: #666; font-size: 1.1rem; }}
        .summary-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 30px 0; }}
        .summary-card {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 25px; border-radius: 15px; text-align: center; }}
        .summary-card .number {{ font-size: 2.5rem; font-weight: bold; }}
        .summary-card .label {{ font-size: 0.9rem; opacity: 0.9; margin-top: 5px; }}
        .email-section {{ background: white; border-radius: 20px; margin-bottom: 30px; overflow: hidden; box-shadow: 0 10px 40px rgba(0,0,0,0.1); }}
        .email-header {{ padding: 30px; border-bottom: 1px solid #eee; }}
        .email-header h2 {{ color: #1a1a2e; font-size: 1.5rem; display: flex; align-items: center; gap: 15px; }}
        .priority-badge {{ padding: 5px 15px; border-radius: 20px; font-size: 0.85rem; font-weight: bold; }}
        .priority-1 {{ background: #fee2e2; color: #dc2626; }}
        .priority-2 {{ background: #fef3c7; color: #d97706; }}
        .priority-3 {{ background: #d1fae5; color: #059669; }}
        .email-meta {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; padding: 20px 30px; background: #f8fafc; }}
        .meta-item {{ }}
        .meta-item .label {{ font-size: 0.85rem; color: #64748b; margin-bottom: 5px; }}
        .meta-item .value {{ font-size: 1rem; color: #1e293b; font-weight: 500; }}
        .email-template {{ padding: 30px; background: #fefce8; border-left: 4px solid #eab308; margin: 20px 30px; border-radius: 10px; }}
        .email-template h3 {{ color: #854d0e; margin-bottom: 15px; font-size: 1.1rem; }}
        .email-template .subject {{ background: white; padding: 15px; border-radius: 8px; margin-bottom: 15px; }}
        .email-template .subject strong {{ color: #64748b; }}
        .email-template .body {{ background: white; padding: 20px; border-radius: 8px; white-space: pre-line; line-height: 1.8; }}
        .customer-table {{ width: 100%; border-collapse: collapse; }}
        .customer-table th {{ background: #f1f5f9; padding: 15px; text-align: left; font-weight: 600; color: #475569; border-bottom: 2px solid #e2e8f0; }}
        .customer-table td {{ padding: 12px 15px; border-bottom: 1px solid #e2e8f0; }}
        .customer-table tr:hover {{ background: #f8fafc; }}
        .customer-table .number {{ text-align: right; }}
        .toggle-btn {{ background: #667eea; color: white; border: none; padding: 10px 20px; border-radius: 8px; cursor: pointer; margin: 20px 30px; font-size: 0.9rem; }}
        .toggle-btn:hover {{ background: #5a67d8; }}
        .customer-list {{ display: none; padding: 0 30px 30px; }}
        .customer-list.show {{ display: block; }}
        .note {{ background: #eff6ff; border-left: 4px solid #3b82f6; padding: 15px 20px; margin: 20px 30px; border-radius: 8px; color: #1e40af; }}
        .strategy-section {{ background: white; border-radius: 20px; padding: 40px; margin-bottom: 30px; box-shadow: 0 10px 40px rgba(0,0,0,0.1); }}
        .strategy-section h2 {{ color: #1a1a2e; margin-bottom: 20px; }}
        .strategy-list {{ list-style: none; }}
        .strategy-list li {{ padding: 15px 0; border-bottom: 1px solid #eee; display: flex; align-items: flex-start; gap: 15px; }}
        .strategy-list li:last-child {{ border-bottom: none; }}
        .strategy-list .icon {{ font-size: 1.5rem; }}
        .strategy-list .content h4 {{ color: #1e293b; margin-bottom: 5px; }}
        .strategy-list .content p {{ color: #64748b; font-size: 0.95rem; }}
        .footer {{ text-align: center; color: white; padding: 20px; opacity: 0.8; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📧 Email Marketing Stratégia - Vevo</h1>
            <p>Obdobie: {date_from.strftime('%d.%m.%Y')} - {date_to.strftime('%Y.%m.%d')}</p>

            <div class="summary-grid">
                <div class="summary-card">
                    <div class="number">{summary.get('total_customers', 0)}</div>
                    <div class="label">Celkom zákazníkov</div>
                </div>
                <div class="summary-card">
                    <div class="number">{summary.get('repeat_rate_pct', 0)}%</div>
                    <div class="label">Miera návratu</div>
                </div>
                <div class="summary-card">
                    <div class="number">{summary.get('true_retention_2nd_pct', summary.get('repeat_rate_pct', 0))}%</div>
                    <div class="label">Skutočná 2. obj. retencia</div>
                </div>
                <div class="summary-card">
                    <div class="number">{summary.get('avg_days_to_2nd_order', 'N/A')}</div>
                    <div class="label">Priem. dní do 2. obj.</div>
                </div>
            </div>
        </div>

        <div class="strategy-section">
            <h2>🎯 Odporúčaná email stratégia</h2>
            <ul class="strategy-list">
                <li>
                    <span class="icon">🧪</span>
                    <div class="content">
                        <h4>1. Konverzia vzoriek (Najvyššia priorita)</h4>
                        <p>Zákazníci, ktorí si kúpili vzorky, by mali dostať email 7-14 dní po nákupe s ponukou na plnú veľkosť. Toto je najdôležitejší segment pre rast tržieb.</p>
                    </div>
                </li>
                <li>
                    <span class="icon">2ď¸ŹâŁ</span>
                    <div class="content">
                        <h4>2. Druhá objednávka (8-14 dní)</h4>
                        <p>Nový zákazníci by mali dostať email s motiváciou k druhej objednávke. Priemerný čas návratu je {summary.get('avg_days_to_2nd_order', 20)} dní.</p>
                    </div>
                </li>
                <li>
                    <span class="icon">🎯</span>
                    <div class="content">
                        <h4>3. Optimálny čas na doplnenie (15-25 dní)</h4>
                        <p>Zákazníci v tomto okne sú ideálni kandidáti na pripomenutie. Sú v "sladkom bode" - parfum pravdepodobne dochádza.</p>
                    </div>
                </li>
                <li>
                    <span class="icon">⚠️</span>
                    <div class="content">
                        <h4>4. Záchrana odchádzajúcich (60-90 dní)</h4>
                        <p>Verní zákazníci, ktorí dlhšie nenakúpili, potrebujú silnejšiu ponuku - 20% zľavu alebo špeciálny darček.</p>
                    </div>
                </li>
                <li>
                    <span class="icon">👑</span>
                    <div class="content">
                        <h4>5. VIP program (3+ objednávky)</h4>
                        <p>Najvernejší zákazníci by mali mať špeciálne výhody - prednostný prístup k novinkám, exkluzívne zľavy, darčeky k objednávkam.</p>
                    </div>
                </li>
            </ul>
        </div>
"""

    # Email templates for each segment
    email_templates = {
        'sample_not_converted': {
            'icon': '🧪',
            'title': 'Konverzia vzoriek na plnú veľkosť',
            'priority': 1,
            'subject': 'KtorĂˇ vĂ´Ĺa VĂˇs najviac oslovila? đźŚ¸ Ĺ peciĂˇlna ponuka pre VĂˇs',
            'body': '''DobrĂ˝ deĹ {meno},

ďakujeme, že ste si vyskúšali naše vzorky parfumov do prania Vevo!

Radi by sme vedeli, ktorĂˇ vĂ´Ĺa sa VĂˇm najviac pĂˇÄŤila? đź’•

Ako poďakovanie za vyskúšanie našich produktov sme pre Vás pripravili špeciálnu ponuku:

đźŽ ZÄ˝AVA 15% na VaĹˇu prvĂş plnĂş veÄľkosĹĄ
Použite kód: MOJAVONA

Najobľúbenejšie vône našich zákazníkov:
â€˘ No.07 Ylang Absolute - luxusnĂˇ kvetinovĂˇ vĂ´Ĺa
• No.09 Pure Garden - sviežosť záhrady
â€˘ No.08 Cotton Dream - jemnĂˇ bavlnenĂˇ vĂ´Ĺa

Platnosť ponuky: 7 dní

S pozdravom,
Tím Vevo

P.S. Máte otázky ohľadom výberu vône? Napíšte nám, radi poradíme! 💬''',
            'timing': '7-14 dní po nákupe vzoriek',
            'discount': '15% na prvú plnú veľkosť'
        },
        'second_order_encouragement': {
            'icon': '2ď¸ŹâŁ',
            'title': 'Motivácia k druhej objednávke',
            'priority': 2,
            'subject': 'Páčil sa Vám náš parfum? 🌟 Máme pre Vás darček',
            'body': '''DobrĂ˝ deĹ {meno},

ďakujeme za Vašu prvú objednávku u nás! Dúfame, že ste spokojní s kvalitou našich produktov.

Keďže ste náš nový zákazník, pripravili sme pre Vás exkluzívnu ponuku:

đźŽ ZÄ˝AVA 10% na VaĹˇu druhĂş objednĂˇvku
Kód: DRUHAOBJ

Čo si obľúbili naši zákazníci:
âś“ IntenzĂ­vna a dlhotrvajĂşca vĂ´Ĺa
✓ Šetrné k bielizni a pokožke
✓ Vydržia až 100+ praní

💡 TIP: Vyskúšajte aj iné vône z našej kolekcie!

Platnosť: 14 dní

S láskou,
Tím Vevo''',
            'timing': '10-12 dní po prvej objednávke',
            'discount': '10% na druhú objednávku'
        },
        'optimal_reorder_timing': {
            'icon': '🎯',
            'title': 'Čas na doplnenie zásob',
            'priority': 2,
            'subject': 'Dochádza Vám parfum do prania? 🧺 Nezabudnite na zásoby',
            'body': '''DobrĂ˝ deĹ {meno},

už je to chvíľa od Vašej poslednej objednávky a možno Vám pomaly dochádza parfum do prania.

Nechceme, aby VaĹˇa bielizeĹ stratila svoju obÄľĂşbenĂş vĂ´Ĺu! đźŚ¸

đźšš DOPRAVA ZADARMO pri objednĂˇvke nad 25€
(PlatĂ­ len tento tĂ˝ĹľdeĹ)

Vaše obľúbené produkty sú pripravené a čakajú na Vás.

Objednajte teraz a ušetrite na doprave!

S pozdravom,
Tím Vevo

P.S. Potrebujete poradiť s výberom? Sme tu pre Vás! 💬''',
            'timing': 'Ihneď - sú v optimálnom okne (15-25 dní)',
            'discount': 'Doprava zadarmo nad 25€'
        },
        'churning_customers': {
            'icon': '⚠️',
            'title': 'Záchrana odchádzajúcich zákazníkov',
            'priority': 1,
            'subject': 'Chýbate nám! 💔 Špeciálna ponuka len pre Vás',
            'body': '''DobrĂ˝ deĹ {meno},

všimli sme si, že ste u nás už dlhšie nenakúpili a úprimne - chýbate nám! 💕

Možno ste našli inú značku, alebo ste len zabudli... Nech je dôvod akýkoľvek, chceme Vás späť!

Preto sme pre Vás pripravili EXKLUZÍVNU ponuku:

đźŽ ZÄ˝AVA 20% na celĂş objednĂˇvku
+ DOPRAVA ZADARMO
Kód: CHYBATEMI

Táto ponuka je len pre Vás a platí iba 7 dní.

Tešíme sa na Vás!

S pozdravom,
Tím Vevo

P.S. Ak ste neboli spokojní s niečím v minulosti, dajte nám vedieť. Radi to napravíme! 🙏''',
            'timing': 'Ihneď - posledná šanca pred stratou',
            'discount': '20% + doprava zadarmo'
        },
        'repeat_buyers_90_days': {
            'icon': '🔄',
            'title': 'Návrat verných zákazníkov',
            'priority': 2,
            'subject': 'Váš obľúbený parfum čaká! 🌸 Špeciálna VIP ponuka',
            'body': '''DobrĂ˝ deĹ {meno},

ďakujeme, že ste naším verným zákazníkom! Vaša podpora pre nás veľa znamená. 💖

Už je to ale dlhšie, čo ste u nás nakúpili, a preto sme pre Vás pripravili špeciálnu VIP ponuku:

👑 VIP ZĽAVA 20%
+ DOPRAVA ZADARMO
+ DARÄŚEK K OBJEDNĂVKE
Kód: VIPZAKAZNIK

Vaše obľúbené vône stále máme skladom a čakajú na Vás!

Platnosť ponuky: 10 dní

S vďakou,
Tím Vevo

P.S. Máte nejaké otázky alebo spätnú väzbu? Budeme radi, ak sa ozvete! 💬''',
            'timing': 'Ihneď - riziko straty zákazníka',
            'discount': '20% + doprava zadarmo + darček'
        },
        'one_time_buyers_30_days': {
            'icon': '🛒',
            'title': 'Re-engagement jednorázových zákazníkov',
            'priority': 3,
            'subject': 'Ako sa Vám páči Vevo parfum? 💕 Máme pre Vás prekvapenie',
            'body': '''DobrĂ˝ deĹ {meno},

dúfame, že ste spokojní s naším parfumom do prania!

Ak ste ho ešte nevyskúšali, je najvyšší čas! A ak áno, určite viete, prečo ho naši zákazníci milujú:

âś“ DlhotrvajĂşca vĂ´Ĺa (aĹľ 100+ pranĂ­)
✓ Šetrné k bielizni aj pokožke
✓ Luxusné vône za dostupnú cenu

Ako poďakovanie za Vašu prvú objednávku máme pre Vás:

đźŽ ZÄ˝AVA 15% na ÄŹalĹˇiu objednĂˇvku
Kód: VERNYSPAT

Platnosť: 14 dní

Tešíme sa na Vás!

S pozdravom,
Tím Vevo''',
            'timing': '30-45 dní po prvej objednávke',
            'discount': '15% na druhú objednávku'
        },
        'high_value_one_time': {
            'icon': '💎',
            'title': 'VIP re-engagement vysokohodnotných zákazníkov',
            'priority': 2,
            'subject': 'Ďakujeme za Vašu veľkú objednávku! 💎 Exkluzívna ponuka',
            'body': '''DobrĂ˝ deĹ {meno},

ešte raz ďakujeme za Vašu nedávnu objednávku! Veľmi si vážime Vašu dôveru v naše produkty.

Keďže ste náš VIP zákazník, pripravili sme pre Vás EXKLUZÍVNU ponuku:

💎 VIP ZĽAVA 15%
+ DOPRAVA ZADARMO
+ PRÉMIOVÉ BALENIE
Kód: VIPKLIENT

Táto ponuka je určená len pre vybraných zákazníkov ako ste Vy.

Čo môžete očakávať:
✓ Rovnaká kvalita, ktorú poznáte
✓ Nové vône v našej kolekcii
✓ Prémiové balenie ako darček

Platnosť: 21 dní

S úctou,
Tím Vevo''',
            'timing': '14-21 dní po prvej objednávke',
            'discount': '15% + doprava zadarmo + prémiové balenie'
        },
        'new_customers_welcome': {
            'icon': '👋',
            'title': 'Privítanie nových zákazníkov',
            'priority': 3,
            'subject': 'Vitajte v rodine Vevo! 🎉 Tipy na používanie parfumu',
            'body': '''DobrĂ˝ deĹ {meno},

vitajte v rodine Vevo! 🎉 Sme veľmi radi, že ste sa rozhodli vyskúšať naše parfumy do prania.

Tu je niekoľko tipov, ako získať z parfumu maximum:

💡 TIPY NA POUŽÍVANIE:
1. Pridajte 1-2 uzávery do bubna práčky
2. Pre intenzĂ­vnejĹˇiu vĂ´Ĺu pridajte aj do avivĂˇĹľe
3. Skladujte na suchom a chladnom mieste

âť“ ÄŚASTO KLADENĂ‰ OTĂZKY:
• Koľko praní vydrží? Až 100+ praní z 200ml fľašky
â€˘ Je vhodnĂ˝ pre citlivĂş pokoĹľku? Ăno, je hypoalergĂ©nny
â€˘ MĂ´Ĺľem kombinovaĹĄ vĂ´ne? Ăno, skĂşĹˇajte!

Ak máte akékoľvek otázky, sme tu pre Vás! Stačí odpovedať na tento email.

Prajeme VĂˇm voĹavĂ© pranie! đźŚ¸

S pozdravom,
Tím Vevo''',
            'timing': '3 dni po doručení objednávky',
            'discount': 'Žiadna zľava - budovanie vzťahu'
        },
        'vip_customers': {
            'icon': '👑',
            'title': 'VIP program pre verných zákazníkov',
            'priority': 4,
            'subject': 'đź‘‘ ExkluzĂ­vne pre VIP: NovĂˇ vĂ´Ĺa eĹˇte pred ostatnĂ˝mi!',
            'body': '''DobrĂ˝ deĹ {meno},

ako náš VIP zákazník s {pocet_objednavok}+ objednávkami ste pre nás veľmi dôležití!

Preto Vám ako prvým predstavujeme NOVINKU v našej kolekcii:

đźŚź [NOVĂ VĂ”Ĺ‡A] - uĹľ ÄŤoskoro!

Ako VIP zákazník máte:
👑 Prednostný prístup k novinkám
👑 Trvalú zľavu 10% na všetky produkty
👑 Doprava zadarmo pri každej objednávke
👑 Darček k každej objednávke

Váš VIP kód: VIPCLUB

Ďakujeme za Vašu vernosť! 💖

S úctou,
Tím Vevo

P.S. MĂˇte nĂˇpad na novĂş vĂ´Ĺu? NapĂ­Ĺˇte nĂˇm, radi vypoÄŤujeme! đź’¬''',
            'timing': 'Pravidelne 1x mesačne',
            'discount': 'Trvalá 10% zľava + doprava zadarmo'
        },
        'failed_payment_only': {
            'icon': '❌',
            'title': 'Záchrana neúspešných platieb',
            'priority': 1,
            'subject': 'Vaša objednávka čaká! 🛒 Pomôžeme Vám dokončiť nákup',
            'body': '''DobrĂ˝ deĹ {meno},

všimli sme si, že sa Vám nepodarilo dokončiť objednávku.

Nechceme, aby VĂˇm uĹˇla prĂ­leĹľitosĹĄ maĹĄ voĹavĂş bielizeĹ! đźŚ¸

Ak ste mali problém s platbou, môžete:
1. Skúsiť inú platobnú kartu
2. Zvoliť platbu na dobierku
3. Kontaktovať nás pre pomoc

đźŽ Ako ospravedlnenie za neprĂ­jemnosti mĂˇme pre VĂˇs:
ZĽAVA 10% na Vašu objednávku
Kód: DOKONCIM

Potrebujete pomoc? Stačí odpovedať na tento email alebo zavolať na [telefón].

S pozdravom,
Tím Vevo

P.S. Vaše produkty sú stále v košíku a čakajú na Vás! 🛒''',
            'timing': '24-48 hodín po neúspešnej platbe',
            'discount': '10% + pomoc s platbou'
        },
        'long_dormant': {
            'icon': '💤',
            'title': 'Reaktivácia dlhodobo neaktívnych',
            'priority': 5,
            'subject': 'UĹľ ste zabudli na voĹavĂş bielizeĹ? đź˘ MĂˇme pre VĂˇs prekvapenie',
            'body': '''DobrĂ˝ deĹ {meno},

je to už dlhšie, čo ste u nás nakúpili. Chýbate nám! 💕

Možno ste našli inú značku, alebo ste jednoducho zabudli... Nech je dôvod akýkoľvek, chceli by sme Vás späť!

Preto sme pre Vás pripravili NAJLEPŠIU ponuku:

đźŽ MEGA ZÄ˝AVA 30%
+ DOPRAVA ZADARMO
+ DARÄŚEK K OBJEDNĂVKE
Kód: CHCEMSPAT

Čo sa za ten čas zmenilo:
✓ Nové vône v kolekcii
✓ Vylepšená receptúra
âś“ EĹˇte dlhĹˇie trvajĂşca vĂ´Ĺa

Táto ponuka platí len 7 dní a je určená špeciálne pre Vás!

Tešíme sa na Váš návrat!

S pozdravom,
Tím Vevo''',
            'timing': 'Ihneď - posledný pokus',
            'discount': '30% + doprava zadarmo + darček'
        },
        'recent_buyers_14_60_days': {
            'icon': '⏰',
            'title': 'Pripomenutie nedávnym zákazníkom',
            'priority': 3,
            'subject': 'Nezabudnite na doplnenie zásob! 🧺 Novinky zo sveta Vevo',
            'body': '''DobrĂ˝ deĹ {meno},

dĂşfame, Ĺľe si uĹľĂ­vate voĹavĂş bielizeĹ s naĹˇimi parfumami! đźŚ¸

Chceli sme Vás informovať o novinkách:

📰 ČO JE NOVÉ:
• Nové vône v kolekcii
• Výhodné balíčky pre rodiny
â€˘ Tipy na starostlivosĹĄ o bielizeĹ

💡 VEDELI STE?
Naše parfumy sú:
✓ Hypoalergénne
✓ Ekologické
✓ Vyrobené na Slovensku

Ak by ste chceli doplniť zásoby, máme pre Vás:

đźšš DOPRAVA ZADARMO nad 30€
(Tento tĂ˝ĹľdeĹ)

Prajeme voĹavĂ˝ deĹ!

S pozdravom,
Tím Vevo''',
            'timing': 'Priebežne podľa dátumu poslednej objednávky',
            'discount': 'Doprava zadarmo nad 30€'
        }
    }

    # Add each email section with customer list
    if customer_email_segments:
        # Sort by priority
        sorted_segments = sorted(customer_email_segments.items(),
                                key=lambda x: x[1].get('priority', 99))

        for segment_name, segment_info in sorted_segments:
            if segment_info['count'] == 0:
                continue

            template = email_templates.get(segment_name, {})
            if not template:
                continue

            priority = segment_info.get('priority', 99)
            priority_class = 'priority-1' if priority <= 2 else ('priority-2' if priority <= 4 else 'priority-3')
            priority_text = 'Vysoká' if priority <= 2 else ('Stredná' if priority <= 4 else 'Nízka')

            segment_data = segment_info['data']

            html_content += f"""
        <div class="email-section">
            <div class="email-header">
                <h2>
                    <span>{template.get('icon', '📧')}</span>
                    {template.get('title', segment_name)}
                    <span class="priority-badge {priority_class}">Priorita: {priority_text}</span>
                </h2>
            </div>

            <div class="email-meta">
                <div class="meta-item">
                    <div class="label">Počet zákazníkov</div>
                    <div class="value" style="font-size: 1.5rem; color: #667eea;">{segment_info['count']}</div>
                </div>
                <div class="meta-item">
                    <div class="label">Kedy poslať</div>
                    <div class="value">{template.get('timing', segment_info.get('send_timing', 'N/A'))}</div>
                </div>
                <div class="meta-item">
                    <div class="label">Odporúčaná zľava</div>
                    <div class="value">{template.get('discount', segment_info.get('discount_suggestion', 'N/A'))}</div>
                </div>
                <div class="meta-item">
                    <div class="label">Účel emailu</div>
                    <div class="value">{segment_info.get('email_purpose', 'N/A')}</div>
                </div>
            </div>

            <div class="email-template">
                <h3>📝 Šablóna emailu</h3>
                <div class="subject">
                    <strong>Predmet:</strong> {template.get('subject', 'N/A')}
                </div>
                <div class="body">{template.get('body', 'Šablóna nie je k dispozícii')}</div>
            </div>

            <div class="note">
                💡 <strong>Tip:</strong> Personalizujte email menom zákazníka. Nahraďte {{meno}} skutočným menom. Testujte rôzne predmety pre vyšší open rate.
            </div>

            <button class="toggle-btn" onclick="toggleCustomerList('{segment_name}')">
                📋 Zobraziť/Skryť zoznam zákazníkov ({segment_info['count']})
            </button>

            <div class="customer-list" id="list-{segment_name}">"""

            # Add customer table if data exists
            if segment_data is not None and not segment_data.empty:
                html_content += """
                <table class="customer-table">
                    <thead>
                        <tr>
                            <th>Email</th>
                            <th>Meno</th>
                            <th class="number">Počet obj.</th>
                            <th class="number">Celková tržba</th>
                            <th class="number">Dní od posl.</th>
                            <th>Mesto</th>
                        </tr>
                    </thead>
                    <tbody>"""

                # Determine which columns to use
                if segment_name == 'failed_payment_only':
                    columns = ['email', 'name', 'failed_order_count', 'last_attempt_date', 'city']
                elif segment_name == 'sample_not_converted':
                    columns = ['email', 'name', 'order_count', 'total_revenue', 'days_since_last', 'city']
                else:
                    columns = ['email', 'name', 'order_count', 'total_revenue', 'days_since_last_order', 'city']

                for _, row in segment_data.head(200).iterrows():
                    email = row.get('email', '') if 'email' in row.index else ''
                    name = row.get('name', '') if 'name' in row.index else ''
                    if pd.isna(name):
                        name = ''

                    order_count = row.get('order_count', row.get('failed_order_count', 0))
                    if pd.isna(order_count):
                        order_count = 0

                    revenue = row.get('total_revenue', 0)
                    if pd.isna(revenue):
                        revenue = 0

                    days = row.get('days_since_last_order', row.get('days_since_last', row.get('days_since_first_order', 0)))
                    if pd.isna(days):
                        days = 0

                    city = row.get('city', '')
                    if pd.isna(city):
                        city = ''

                    html_content += f"""
                        <tr>
                            <td>{email}</td>
                            <td>{name}</td>
                            <td class="number">{int(order_count)}</td>
                            <td class="number">€{float(revenue):.2f}</td>
                            <td class="number">{int(days)}</td>
                            <td>{city}</td>
                        </tr>"""

                if len(segment_data) > 200:
                    html_content += f"""
                        <tr style="background: #fef3c7;">
                            <td colspan="6" style="text-align: center; font-style: italic;">
                                ... a ďalších {len(segment_data) - 200} zákazníkov. Kompletný zoznam v CSV súbore.
                            </td>
                        </tr>"""

                html_content += """
                    </tbody>
                </table>"""

            html_content += """
            </div>
        </div>
"""

    html_content += f"""
        <div class="footer">
            <p>Vygenerované: {datetime.now().strftime('%d.%m.%Y %H:%M')} | Vevo Email Marketing Stratégia</p>
            <p>📧 Pre export emailov použite CSV súbory v priečinku data/</p>
        </div>
    </div>

    <script>
        function toggleCustomerList(segmentName) {{
            const list = document.getElementById('list-' + segmentName);
            list.classList.toggle('show');
        }}
    </script>
</body>
</html>
"""

    return html_content


