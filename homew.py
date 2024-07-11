import requests
import psycopg2
import json  


def connect_db():
    return psycopg2.connect(
        dbname='n445',
        user='postgres',
        password='123',
        host='localhost',
        port='5432'
    )


def create_table():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS products (
        id SERIAL PRIMARY KEY,
        product_id INT,
        title VARCHAR(255),
        description TEXT,
        price DECIMAL(10, 2),
        discountPercentage DECIMAL(5, 2),
        rating DECIMAL(3, 2),
        stock INT,
        brand VARCHAR(255),
        category VARCHAR(255),
        tags JSONB  -- JSONB turida tags ma'lumotlarni saqlaymiz
    );
    """
  
    connection = connect_db()
    cursor = connection.cursor()
    cursor.execute(create_table_query)
    connection.commit()



def save_product(data):
        connection = connect_db()
        cursor = connection.cursor()
        insert_query = """
        INSERT INTO products (product_id, title, description, price, discountPercentage, rating, stock, brand, category, tags)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        for product in data:
            cursor.execute(insert_query, (
                product['id'], 
                product['title'], 
                product['description'], 
                product['price'], 
                product['discountPercentage'], 
                product['rating'], 
                (product['stock']),  
                product['brand'], 
                product['category'], 
                product['tags'])  
            )
        connection.commit()
    


def save():
    save_prod = requests.get('https://dummyjson.com/products')
    if save_prod.status_code == 200:
        data = save_prod.json()['products']
        save_product(data)
        print('all codes checked ')
    else:
        print(f"it has a error: {save_prod.status_code}")


    create_table()  
    save()  