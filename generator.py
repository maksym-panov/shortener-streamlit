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
UNIQUE_IPS_COUNT = 50000
START_DATE = datetime.now() - timedelta(days=90)

print(f"Starting generation of {NUM_CLICKS} records...")
print(f"Region: Only Europe | Unique IPs: {UNIQUE_IPS_COUNT}")

user_ids = [str(uuid.uuid4()) for _ in range(NUM_USERS)]
domains = ['github.com', 'google.com', 'medium.com', 'youtube.com', 'aws.amazon.com', 'dou.ua', 'rozetka.com.ua', 'ukr.net']

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
        'is_active': np.random.choice([True, False], p=[0.95, 0.05])
    })
    short_codes_list.append(short_code)

df_urls = pd.DataFrame(urls_data)
df_urls.to_csv('urls_metadata.csv', index=False)
print("urls_metadata.csv created.")

europe_map = {
    'UA': 0.50, 'PL': 0.15, 'DE': 0.10, 'GB': 0.05, 'NL': 0.04,
    'FR': 0.04, 'ES': 0.03, 'IT': 0.03, 'SE': 0.02, 'CZ': 0.02, 'RO': 0.02
}
country_codes = list(europe_map.keys())
country_weights = np.array(list(europe_map.values()))
country_weights /= country_weights.sum()

generated_countries = np.random.choice(country_codes, size=NUM_CLICKS, p=country_weights)

ip_pool = [fake.ipv4() for _ in range(UNIQUE_IPS_COUNT)]
generated_ips = np.random.choice(ip_pool, size=NUM_CLICKS)

popular_urls = np.random.choice(short_codes_list, size=int(NUM_URLS * 0.2), replace=False)
regular_urls = list(set(short_codes_list) - set(popular_urls))

urls_choice = np.concatenate([
    np.random.choice(popular_urls, size=int(NUM_CLICKS * 0.8)),
    np.random.choice(regular_urls, size=int(NUM_CLICKS * 0.2))
])
np.random.shuffle(urls_choice)

hour_probs = [
    0.005, 0.005, 0.005, 0.005, 0.01, 0.02,
    0.04, 0.06, 0.08, 0.09, 0.09, 0.08,
    0.08, 0.08, 0.07, 0.07, 0.06, 0.05,
    0.05, 0.04, 0.03, 0.02, 0.01, 0.01
]
hour_probs = np.array(hour_probs)
hour_probs /= hour_probs.sum()

random_days = np.random.randint(0, (datetime.now() - START_DATE).days, size=NUM_CLICKS)
random_hours = np.random.choice(range(24), size=NUM_CLICKS, p=hour_probs)
random_minutes = np.random.randint(0, 60, size=NUM_CLICKS)

base_dates = np.array([START_DATE + timedelta(days=int(d)) for d in random_days])
final_dates = base_dates + \
              np.array([timedelta(hours=int(h)) for h in random_hours]) + \
              np.array([timedelta(minutes=int(m)) for m in random_minutes])

normal_latency = np.random.lognormal(mean=3.9, sigma=0.4, size=int(NUM_CLICKS * 0.85))
lag_latency = np.random.lognormal(mean=6.0, sigma=0.6, size=int(NUM_CLICKS * 0.15))

latencies = np.concatenate([normal_latency, lag_latency])
latencies = np.clip(latencies, 5, 3000).astype(int)
np.random.shuffle(latencies)

platforms = np.random.choice(['Mobile', 'Desktop', 'Tablet'], size=NUM_CLICKS, p=[0.6, 0.35, 0.05])
status_codes = np.random.choice([200, 301, 404, 500], size=NUM_CLICKS, p=[0.1, 0.85, 0.04, 0.01])
user_agents_pool = [fake.user_agent() for _ in range(50)]
user_agents = np.random.choice(user_agents_pool, size=NUM_CLICKS)

df_clicks = pd.DataFrame({
    'event_id': [str(uuid.uuid4()) for _ in range(NUM_CLICKS)],
    'short_code': urls_choice,
    'timestamp': final_dates,
    'ip_address': generated_ips,
    'user_agent': user_agents,
    'referrer': np.random.choice(['Google', 'Facebook', 'Direct', 'Telegram', 'Instagram'], size=NUM_CLICKS),
    'country_code': generated_countries,
    'latency_ms': latencies,
    'status_code': status_codes,
    'platform': platforms
})

df_clicks.sort_values(by='timestamp', inplace=True)

df_clicks.to_csv('clicks_stream.csv', index=False)
print(f"clicks_stream.csv created ({len(df_clicks)} records).")
print(f"Stats:")
print(f"   - Latency Max: {df_clicks['latency_ms'].max()} ms")
print(f"   - Latency Mean: {df_clicks['latency_ms'].mean():.2f} ms")
print(f"   - Unique IPs: {df_clicks['ip_address'].nunique()}")
print("Done.")