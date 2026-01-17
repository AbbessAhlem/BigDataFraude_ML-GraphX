#!/usr/bin/env python3
"""
Kafka Producer - Simulates real-time credit card transactions
"""

import json
import time
import random
import sys

try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
except ImportError:
    print("❌ kafka-python not installed!")
    print("Run: pip install kafka-python")
    sys.exit(1)

try:
    import pandas as pd
except ImportError:
    print("❌ pandas not installed!")
    print("Run: pip install pandas")
    sys.exit(1)

# Kafka Configuration
KAFKA_BROKER = 'kafka:9092'
KAFKA_TOPIC = 'credit-card-transactions'

print("=" * 70)
print("🚀 KAFKA PRODUCER - Credit Card Transaction Simulator")
print("=" * 70)

# Initialize Kafka Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(2, 5, 0)
    )
    print(f"✅ Connected to Kafka: {KAFKA_BROKER}")
    print(f"📤 Publishing to topic: {KAFKA_TOPIC}\n")
except KafkaError as e:
    print(f"❌ Failed to connect to Kafka: {e}")
    print("\n💡 Make sure Kafka is running:")
    print("   docker ps | grep kafka")
    sys.exit(1)

# Load transaction data
try:
    df = pd.read_csv('/home/jovyan/data/creditcard.csv')
    print(f"✅ Loaded {len(df)} transactions from CSV\n")
except Exception as e:
    print(f"❌ Failed to load CSV: {e}")
    sys.exit(1)

# Stream transactions
print("=" * 70)
print("🟢 STREAMING TRANSACTIONS TO KAFKA")
print("=" * 70)
print("Press Ctrl+C to stop\n")

try:
    count = 0
    fraud_count = 0
    
    while True:
        # Get random transaction
        transaction = df.sample(n=1).iloc[0].to_dict()
        
        # Add timestamp
        transaction['timestamp'] = time.time()
        
        # Convert numpy types to Python types
        for key, value in transaction.items():
            if hasattr(value, 'item'):
                transaction[key] = value.item()
        
        # Send to Kafka
        try:
            future = producer.send(KAFKA_TOPIC, transaction)
            record_metadata = future.get(timeout=10)
            
            count += 1
            if transaction['Class'] == 1:
                fraud_count += 1
            
            status = "🚨 FRAUD" if transaction['Class'] == 1 else "✅ Normal"
            amount = transaction['Amount']
            
            if count % 10 == 0:
                print(f"📊 Sent: {count} txns | Frauds: {fraud_count} | Latest: ${amount:.2f} {status}")
        
        except KafkaError as e:
            print(f"❌ Failed to send: {e}")
        
        # Random delay between transactions
        time.sleep(random.uniform(0.1, 1.0))

except KeyboardInterrupt:
    print(f"\n\n🛑 Stopping producer...")
    print(f"\n📊 Statistics:")
    print(f"   Total transactions sent: {count}")
    print(f"   Fraud transactions: {fraud_count}")
    print(f"   Fraud rate: {fraud_count/count*100:.2f}%")
    producer.close()
    print("\n✅ Producer closed")
