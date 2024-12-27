# src/integrations/powerbi_connector.py
import pandas as pd
import boto3
import pyodbc
from datetime import datetime, timedelta
import json

class PowerBIConnector:
    def __init__(self, redshift_connection_string):
        self.redshift_conn = pyodbc.connect(redshift_connection_string)
        self.s3 = boto3.client('s3')
    
    def fetch_cost_data(self):
        """
        Fetch cost analysis data from Redshift with proper structure for Power BI
        """
        query = """
        WITH shipping_costs AS (
            SELECT 
                DATE_TRUNC('day', s.ship_date) as ship_date,
                a.account_name,
                r.region_name,
                s.service_type,
                s.shipping_cost,
                s.fuel_surcharge,
                s.additional_handling,
                s.weight_lbs,
                COUNT(*) as shipment_count
            FROM fact_shipments s
            JOIN dim_accounts a ON s.account_key = a.account_key
            JOIN dim_regions r ON s.region_key = r.region_key
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
        )
        SELECT 
            ship_date,
            account_name,
            region_name,
            service_type,
            shipping_cost,
            fuel_surcharge,
            additional_handling,
            weight_lbs,
            shipment_count,
            (shipping_cost + fuel_surcharge + additional_handling) as total_cost,
            (shipping_cost + fuel_surcharge + additional_handling)/weight_lbs as cost_per_pound
        FROM shipping_costs
        ORDER BY ship_date DESC
        """
        
        return pd.read_sql(query, self.redshift_conn)
    
    def prepare_for_powerbi(self, df):
        """
        Prepare data with proper hierarchies and relationships for Power BI
        """
        # Add date dimensions for better drill-down
        df['Year'] = df['ship_date'].dt.year
        df['Quarter'] = df['ship_date'].dt.quarter
        df['Month'] = df['ship_date'].dt.month
        df['Week'] = df['ship_date'].dt.isocalendar().week
        
        # Add cost brackets for better analysis
        df['cost_bracket'] = pd.qcut(df['total_cost'], q=5, labels=[
            'Very Low', 'Low', 'Medium', 'High', 'Very High'
        ])
        
        # Add weight categories
        df['weight_category'] = pd.cut(df['weight_lbs'], 
            bins=[0, 10, 25, 50, 100, float('inf')],
            labels=['Light', 'Medium', 'Heavy', 'Very Heavy', 'Extra Heavy']
        )
        
        return df
    
    def export_to_csv(self, df, output_path):
        """
        Export prepared data to CSV for Power BI import
        """
        df.to_csv(output_path, index=False)
        print(f"Data exported to {output_path}")
