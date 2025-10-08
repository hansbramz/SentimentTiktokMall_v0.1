# main.py
import os
import requests
import json
import pandas as pd
import nltk
import time
from datetime import date, datetime
from deep_translator import GoogleTranslator
from transformers import pipeline
from dotenv import load_dotenv
from utils.db_connector import setup_database

# Load environment variables from .env file
load_dotenv()

# Load database credentials from .env
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT"))
DB_DATABASE = os.getenv("DB_DATABASE")

# Use a fallback empty string to prevent the NoneType error
cookies_json_str = os.getenv("COOKIES_JSON", '{}')
COOKIES = json.loads(cookies_json_str)

headers_json_str = os.getenv("HEADERS_JSON", '{}')
HEADERS = json.loads(headers_json_str)

# Setup NLP pipelines
print("Downloading NLTK vader_lexicon...")
nltk.download('vader_lexicon', quiet=True)
print("Loading sentiment and emotion models...")
sentiment_analyzer = pipeline("sentiment-analysis", model="w11wo/indonesian-roberta-base-sentiment-classifier")
emotion_classifier = pipeline("text-classification", model="StevenLimcorn/indonesian-roberta-base-emotion-classifier")

# Helper function
def convert_timestamp_to_date(timestamp):
    """Converts a Unix timestamp (in milliseconds) to a YYYY-MM-DD string."""
    if timestamp is None:
        return None
    try:
        timestamp_in_seconds = int(timestamp) / 1000
        dt_object = datetime.fromtimestamp(timestamp_in_seconds)
        return dt_object.strftime('%Y-%m-%d')
    except (ValueError, TypeError) as e:
        print(f"Error converting timestamp {timestamp}: {e}")
        return None

def fetch_and_process_reviews():
    """Fetches reviews, processes them, and returns a DataFrame."""
    json_data = {
        'page': 1,
        'size': 100,
    }

    print("Fetching data from the API...")
    try:
        response = requests.post(
            'https://seller-id.tokopedia.com/api/v1/review/biz_backend/list',
            cookies=COOKIES,
            headers=HEADERS,
            json=json_data,
            params={
                'locale': 'id-ID', 'language': 'id', 'oec_seller_id': '7494665915162987268',
                'aid': '4068', 'app_name': 'i18n_ecom_shop',
                'fp': 'verify_mg7fogjh_7ZXZqRSY_jBj7_4LQu_9B93_VPUohKaS2F8y',
                'device_platform': 'web', 'cookie_enabled': 'true',
                'screen_width': '1494', 'screen_height': '934',
                'browser_language': 'en-US', 'browser_platform': 'Win32',
                'browser_name': 'Mozilla', 'browser_version': '5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
                'browser_online': 'true', 'timezone_name': 'Asia/Jakarta',
                'msToken': 'YTKC0LDGA8uzUU41_VEiv0YA5ljcLIg1_nNHLyVZWXDluoV2584pKlu-pcG4Hj-4UAx8S0YraEcX4USJB6ASK2zm3m9s6H608PhNoW8cm7l7nQBgemFayeeIsHRWhw==',
                'X-Bogus': 'DFSzswVL7CHIHsudC9EX0uz5r0V6',
                'X-Gnarly': 'MaXWRUmRpwXUm9yq7tyeKQVr25e-jBJNk77NVgE9evkc5DWCPLKmYL/PTq6ZeLh5/z5C3b2h3i-wtWGcsYWC29Iupl-a5F3NrsJVcfe/raiHpGHjYT8EVseHQ/DXv8HWfQ1e7uc0vKoKWtGX6kmqGnzZgzOq/aJvsSbN5zfcMm5KonfPSInXet0R-gu58ic3woSOkatxy3z3Z6bkYj405erf5gcLh5TNHY/KsHUuMYAbyUcoRJhzSW9u-nn9yMM/Jnz6/7irma6Z'
            }
        )
        response.raise_for_status()
        reviews = response.json().get('data', {}).get('list', [])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return pd.DataFrame()

    hasil = []
    today = date.today()

    for i in reviews:
        try:
            review_text = i.get('review_text')

            sentiment_label, sentiment_score, emotion_label, emotion_score = "neutral", 0.0, "neutral", 0.0
            if isinstance(review_text, str) and review_text.strip():
                sentiment_result = sentiment_analyzer(review_text)[0]
                sentiment_label = sentiment_result['label']
                sentiment_score = sentiment_result['score']

                emotion_result = emotion_classifier(review_text)[0]
                emotion_label = emotion_result['label']
                emotion_score = emotion_result['score']
            else:
                print(f"Skipping analysis for review {i.get('main_review_id')} due to empty text.")
                
            hasil.append({
                'reviewid': i.get('main_review_id'),
                'order_id': i.get('order_id'),
                'reply_date': convert_timestamp_to_date(i.get('reply_time')),
                'review_text': review_text,
                'rating': i.get('star_level'),
                'review_date': convert_timestamp_to_date(i.get('review_time')),
                'username': i.get('user_name'),
                'product_name': i.get('product_info', {}).get('product_name'),
                'product_id': 'TiktokMall' + str(i.get('product_info', {}).get('product_id')), # Assuming product_id can be converted to string
                'product_image': i.get('product_info', {}).get('img', {}).get('url_list', [None])[0],
                'sentiment_label': sentiment_label,
                'sentiment_score': sentiment_score,
                'emotion_label': emotion_label,
                'emotion_score': emotion_score,
                'sku_specification': i.get('product_info', {}).get('sku_specification'),
                'sales_channel': 'TiktokMall',
                'Brand': 'Bodypack',
                'TanggalScrape': today.strftime('%Y-%m-%d')
            })
        except Exception as e:
            print(f"Error processing a review: {e}")
            continue

    df = pd.DataFrame(hasil)
    return df

def main():
    """Main function to run the script."""
    # Setup database
    engine = setup_database(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_DATABASE)
    if not engine:
        print("Exiting due to database connection error.")
        return

    # Fetch and process data
    df = fetch_and_process_reviews()
    if df.empty:
        print("No data to append. Exiting.")
        return

    # Write DataFrame to the database
    print("Appending data to the database...")
    try:
        df.to_sql(name='ProductReview', con=engine, if_exists='append', index=False, dtype={
            'review_date': Date(),
            'TanggalScrape': Date()
        })
        print('Data appended successfully! 🎉')
    except Exception as e:
        print(f"Error appending data to database: {e}")

if __name__ == "__main__":
    main()
