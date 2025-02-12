import sqlite3
import pandas as pd

def table_creation():
    connection = sqlite3.connect("star_schema.db")
    cursor = connection.cursor()
    
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS DimCustomer (
            CustomerID INTEGER PRIMARY KEY,
            Name TEXT,
            Age INTEGER
        )
        """
    )
    
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS DimBook (
            BookID INTEGER PRIMARY KEY,
            BookName TEXT,
            Genre TEXT,
            Price REAL
        )
        """
    )
    
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS DimDate (
            DateID INTEGER PRIMARY KEY,
            Date TEXT
        )
        """
    )
    
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS FactSales (
            SaleID INTEGER PRIMARY KEY,
            BookID INTEGER,
            CustomerID INTEGER,
            DateID INTEGER,
            FOREIGN KEY (CustomerID) REFERENCES DimCustomer(CustomerID),
            FOREIGN KEY (BookID) REFERENCES DimBook(BookID),
            FOREIGN KEY (DateID) REFERENCES DimDate(DateID)
        )
        """
    )
    
    query = """
        SELECT 
            FactSales.SaleID, 
            DimCustomer.Name AS Customer, 
            DimBook.BookName AS Book, 
            DimDate.Date 
        FROM FactSales 
        JOIN DimCustomer ON FactSales.CustomerID = DimCustomer.CustomerID 
        JOIN DimBook ON FactSales.BookID = DimBook.BookID 
        JOIN DimDate ON FactSales.DateID = DimDate.DateID
    """
    
    df = pd.read_sql_query(query, connection)
    print(df)
    
    connection.close()

def insert_sample_data():
    connection = sqlite3.connect("star_schema.db")
    cursor = connection.cursor()

    cursor.execute("DELETE FROM FactSales")
    cursor.execute("DELETE FROM DimCustomer")
    cursor.execute("DELETE FROM DimBook")
    cursor.execute("DELETE FROM DimDate")

    cursor.execute("INSERT INTO DimCustomer (CustomerID, Name, Age) VALUES (1, 'Arki', 22)")
    
    cursor.execute("INSERT INTO DimBook (BookID, BookName, Genre, Price) VALUES (1, 'Blood Meridian', 'Fiction', 10.99)")
    
    cursor.execute("INSERT INTO DimDate (DateID, Date) VALUES (1, '01-01-2025')")
    
    cursor.execute("INSERT INTO FactSales (SaleID, BookID, CustomerID, DateID) VALUES (1, 1, 1, 1)")

    connection.commit()
    connection.close()

def main():
    table_creation()
    insert_sample_data()

if __name__ == "__main__":
    main()
