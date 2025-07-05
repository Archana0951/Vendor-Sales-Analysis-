import pandas as pd
import sqlite3
import logging
import time 
import numpy as np
from Ingestion_db import ingest_db

logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s", 
    force=True,
    filemode="a"
)

def create_vendor_summary(conn):
    '''This function will merge the different tables to get the overall vendor summary and adding new columns in the resultant data'''
    vendor_sales_summary = pd.read_sql_query("""WITH FreightSummary AS (
    SELECT
    VendorNumber,
    SUM(Freight) AS FreightCost
    FROM Vendor_invoice
    GROUP BY VendorNumber
    ),
    PurchaseSummary AS (
    SELECT
    p.VendorNumber,
    p.VendorName,
    p.Brand,
    p.Description,
    p.PurchasePrice,
    pp.Price AS ActualPrice,
    pp.Volume,
    SUM(p.Quantity) AS TotalPurchaseQuantity,
    SUM(p.Dollars) AS TotalPurchaseDollars
    FROM purchases p
    JOIN purchase_prices pp
    ON p.Brand = pp.Brand
    WHERE p.PurchasePrice > 0
    GROUP BY 
    p.VendorNumber, 
    p.VendorName,
    p.Brand,
    p.Description,
    p.PurchasePrice,
    pp.Price,
    pp.Volume
    ),
    SalesSummary AS (
    SELECT
    VendorNo,
    Brand,
    SUM(SalesQuantity) AS TotalSalesQuantity,
    SUM(SalesDollars) AS TotalSalesDollars,
    SUM(SalesPrice) AS TotalSalesPrice,
    SUM(ExciseTax) AS TotalExciseTax
    FROM sales
    GROUP BY VendorNo, Brand
    )
    SELECT
    ps.VendorNumber,
    ps.VendorName,
    ps.Brand,
    ps.Description,
    ps.ActualPrice,
    ps.Volume,
    ps.TotalPurchaseQuantity,
    ps.TotalPurchaseDollars,
    ss.TotalSalesQuantity,
    ss.TotalSalesPrice,
    ss.TotalSalesDollars,
    ss.TotalExciseTax,
    fs.FreightCost
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss
    ON ps.VendorNumber = ss.VendorNo
    AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs
    ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC;
    """, conn)
    
    return vendor_sales_summary


def clean_data(df):
    '''This Function will clean the data'''
    #Changing datatype to float
    df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce')

    #Filling missing Values with 0
    df.fillna(0, inplace=True)

    #Removing spaces from categorical columns
    df['VendorName'] = df['VendorName'].astype(str).str.strip()
    df['Description'] = df['Description'].astype(str).str.strip()

    #Creating new columns for analysis
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = np.where(df['TotalSalesDollars'] != 0, 
                                  (df['GrossProfit'] / df['TotalSalesDollars']) * 100, 0)
    df['StockTurnOver'] = np.where(df['TotalPurchaseQuantity'] != 0, 
                                   df['TotalSalesQuantity'] / df['TotalPurchaseQuantity'], 0)
    df['SalesPurchaseRatio'] = np.where(df['TotalPurchaseDollars'] != 0, 
                                        df['TotalSalesDollars'] / df['TotalPurchaseDollars'], 0)
    
    return df


if __name__ == '__main__':
    #creating database connection
    conn = sqlite3.connect('inventory.db')

    logging.info('Creating vendor summary Table...')
    summary_df = create_vendor_summary(conn)
    logging.info('Vendor Summary Table created successfully.')
    logging.info('Top 5 rows before cleaning:')
    logging.info('\n%s', summary_df.head().to_string(index=False))

    logging.info('Cleaning Data...')
    clean_df = clean_data(summary_df)
    logging.info('Data cleaned successfully.')
    logging.info('Top 5 rows after cleaning:')
    logging.info('\n%s', clean_df.head().to_string(index=False))

    logging.info('Ingesting data...')
    ingest_db(clean_df, 'vendor_sales_summary', conn)
    logging.info('Data ingestion completed.')
