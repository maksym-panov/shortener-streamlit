import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import uuid

fake = Faker()
Faker.seed(42)
np.random.seed(42)

NUM_USERS = 150
NUM_URLS = 1000
NUM_CLICKS = 200000
START_DATE = datetime.now() - timedelta(days=90)

print(f"Started generation of {NUM_CLICKS} records...")

print(f"1. Creating base of users and links...")

user_ids = [str(uuid.uuid4()) for _ in range(NUM_USERS)]
domains = ['github.com', 'google.com', 'medium.com', 'youtube.com', 'aws.amazon.com', 'dou.ua', 'rozetka.com.ua',
           'stackoverflow.com']

urls_data = []
short_codes_list = []

for _ in range(NUM_URLS):
    created_at = fake.date_time_between(start_date=START_DATE, end_date='now')
    original_url = f"https://{random.choice(domains)}/{fake.slug()}"
    short_code = fake.bothify(text='????##')

    urls_data.append({
        'url_id': str(uuid.uuid4()),
        'short_code': short_code,
        'original_url': original_url,
        'user_id': random.choice(user_ids),
        'created_at': created_at,
        'expiration_date': created_at + timedelta(days=365),
        'is_active': np.random.choice([True, False], p=[0.9, 0.1])
    })
    short_codes_list.append(short_code)

df_urls = pd.DataFrame(urls_data)
df_urls.to_csv('urls_metadata.csv', index=False)
print("urls_metadata.csv створено.")

print(f"2. Generating clicks stream (Smart Geo Distribution)...")

countries_map = {
    'UA': 0.45,
    'PL': 0.10,
    'US': 0.10,
    'DE': 0.08,
    'GB': 0.05,
    'NL': 0.04,
    'CA': 0.03,
    'FR': 0.03,
    'ES': 0.02,
    'IT': 0.02,
    'TR': 0.01,
    'IN': 0.01,
    'JP': 0.01,
    'BR': 0.01,
    'AU': 0.01,
    'CN': 0.005, 'SE': 0.005, 'NO': 0.005, 'FI': 0.005, 'MX': 0.005, 'KZ': 0.005
}

country_codes = list(countries_map.keys())
country_weights = list(countries_map.values())

country_weights = np.array(country_weights)
country_weights /= country_weights.sum()

generated_countries = np.random.choice(country_codes, size=NUM_CLICKS, p=country_weights)

popular_urls = np.random.choice(short_codes_list, size=int(NUM_URLS * 0.2), replace=False)
regular_urls = list(set(short_codes_list) - set(popular_urls))

urls_choice = np.concatenate([
    np.random.choice(popular_urls, size=int(NUM_CLICKS * 0.8)),
    np.random.choice(regular_urls, size=int(NUM_CLICKS * 0.2))
])
np.random.shuffle(urls_choice)

time_deltas = np.random.randint(0, int((datetime.now() - START_DATE).total_seconds()), size=NUM_CLICKS)
base_dates = np.array([START_DATE] * NUM_CLICKS) + np.array([timedelta(seconds=int(x)) for x in time_deltas])

latencies = np.random.lognormal(mean=4.2, sigma=0.7, size=NUM_CLICKS).astype(int)

platforms = np.random.choice(['Mobile', 'Desktop', 'Tablet'], size=NUM_CLICKS, p=[0.55, 0.40, 0.05])
status_codes = np.random.choice([200, 301, 404, 500], size=NUM_CLICKS, p=[0.05, 0.90, 0.04, 0.01])
user_agents_pool = [fake.user_agent() for _ in range(100)]
user_agents = np.random.choice(user_agents_pool, size=NUM_CLICKS)

df_clicks = pd.DataFrame({
    'event_id': [str(uuid.uuid4()) for _ in range(NUM_CLICKS)],
    'short_code': urls_choice,
    'timestamp': base_dates,
    'ip_address': [fake.ipv4() for _ in range(NUM_CLICKS)],
    'user_agent': user_agents,
    'referrer': np.random.choice(['Google', 'Facebook', 'Direct', 'Twitter', 'Instagram'], size=NUM_CLICKS),
    'country_code': generated_countries,
    'latency_ms': latencies,
    'status_code': status_codes,
    'platform': platforms
})

df_clicks.sort_values(by='timestamp', inplace=True)

df_clicks.to_csv('clicks_stream.csv', index=False)
print(f"clicks_stream.csv created ({len(df_clicks)} records).")
print(f"Countries stats:")
print(df_clicks['country_code'].value_counts(normalize=True).head(5))
print("Done!")