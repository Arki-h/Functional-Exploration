import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import random
import kagglehub
import chardet
import csv

def table_creation():
    connection = sqlite3.connect("star_schema.db")
    cursor = connection.cursor()
    
    cursor.execute("DROP TABLE IF EXISTS FactSales")
    cursor.execute("DROP TABLE IF EXISTS DimCustomer")
    cursor.execute("DROP TABLE IF EXISTS DimBook")
    cursor.execute("DROP TABLE IF EXISTS DimDate")

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
            Publisher TEXT,
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

def data_gen(num_id):
    # Fact table
    id_gen = [x for x in range(num_id)]

    #Date dimension
    start_date = datetime(2020,1 ,1)
    end_date = datetime(2026, 1, 1)

    date_diff = end_date - start_date

    date_array = []

    for i in range(num_id):
        #date_array.append(id_gen[i])

        rand_day = random.randint(0, date_diff.days)
        random_date = start_date + timedelta(days=rand_day)

        date_array.append(random_date)

    #Book dimension

    path = kagglehub.dataset_download("saurabhbagchi/books-dataset")
    print("path: ", path)

    file_path = r"C:\Users\arkih\.cache\kagglehub\datasets\saurabhbagchi\books-dataset\versions\1\books_data\books.csv"

    with open(file_path, "rb") as f:
        result = chardet.detect(f.read(100000))  # Read first 100,000 bytes
        print(result["encoding"])

    df = pd.read_csv(file_path, encoding="ISO-8859-1", sep=";", quoting=csv.QUOTE_NONE, on_bad_lines="skip")
    row_count = len(df)
    print("amount of rows: {}".format(row_count))

    print(df.columns)
    df_book = df['"Book-Title"']
    df_publisher = df['"Publisher"']

    #random book selection

    #random_book_id = (random.randint(0, 250012)) + 1
    #print("Random book id: {}".format(random_book_id))

    #random_book = df_book.iloc[random_book_id]
    #print("Random chosen book: {}".format(random_book))

    book_arr = []
    publisher_arr = []
    price_set = {8, 9.99, 12.50, 25}
    price_arr = []

    for i in range(num_id):
        random_book_id = (random.randint(0, 250012)) + 1
        
        random_book = df_book.iloc[random_book_id]
        random_publisher = df_publisher.iloc[random_book_id]
        random_price = price_set.pop()
        price_set.add(random_price)

        book_arr.append(random_book)
        publisher_arr.append(random_publisher)
        price_arr.append(random_price)
        print("Book chosen: {}".format(book_arr[i]))
        print("Book's publisher: {}".format(publisher_arr[i]))
        print("Book's price: {}".format(price_arr[i]))

    return

def insert_sample_data():
    connection = sqlite3.connect("star_schema.db")
    cursor = connection.cursor()

    cursor.execute("DELETE FROM FactSales")
    cursor.execute("DELETE FROM DimCustomer")
    cursor.execute("DELETE FROM DimBook")
    cursor.execute("DELETE FROM DimDate")

    cursor.execute("INSERT INTO DimCustomer (CustomerID, Name, Age) VALUES (1, 'Arki', 22)")
    
    cursor.execute("INSERT INTO DimBook (BookID, BookName, Publisher, Price) VALUES (1, 'Blood Meridian', 'Fiction', 10.99)")
    
    cursor.execute("INSERT INTO DimDate (DateID, Date) VALUES (1, '01-01-2025')")
    
    cursor.execute("INSERT INTO FactSales (SaleID, BookID, CustomerID, DateID) VALUES (1, 1, 1, 1)")

    connection.commit()
    connection.close()

def main():
    table_creation()
    data_gen(10)
    insert_sample_data()

if __name__ == "__main__":
    main()
