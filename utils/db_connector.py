# utils/db_connector.py
import pymysql
from sqlalchemy import create_engine, Column, String, Integer, Text, Date, MetaData, Table, Float
from sqlalchemy.dialects.mysql import VARCHAR

def setup_database(user, passw, host, port, database):
    """
    Sets up the database connection and creates the necessary table if it doesn't exist.
    """
    try:
        # Create database if it does not exist
        conn = pymysql.connect(host=host, port=port, user=user, passwd=passw)
        conn.cursor().execute(f"CREATE DATABASE IF NOT EXISTS {database}")
        conn.close()

        # Create the SQLAlchemy engine
        connection_string = f"mysql+pymysql://{user}:{passw}@{host}:{port}/{database}"
        engine = create_engine(connection_string)

        # Define the table metadata
        metadata = MetaData()
        reviews_table = Table(
            'ProductReview', metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('reviewid', VARCHAR(255)),
            Column('order_id', VARCHAR(255)),
            Column('reply_date', Date),
            Column('review_text', Text),
            Column('rating', Integer),
            Column('review_date', Date),
            Column('username', VARCHAR(255)),
            Column('product_name', VARCHAR(255)),
            Column('product_id', VARCHAR(255)),
            Column('product_image', VARCHAR(500)),
            Column('sentiment_label', VARCHAR(255)),
            Column('sentiment_score', Float),
            Column('emotion_label', VARCHAR(255)),
            Column('emotion_score', Float),
            Column('sku_specification', VARCHAR(255)),
            Column('sales_channel', VARCHAR(255)),
            Column('Brand', VARCHAR(255)),
            Column('TanggalScrape', Date)
        )

        # Create the table in the database if it does not exist
        metadata.create_all(engine)
        print("Database and table setup complete.")
        return engine

    except Exception as e:
        print(f"Error during database setup: {e}")
        return None
