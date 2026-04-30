"""
Example: Fraud Detection with Stateful Processing
"""
import liutang
import random

FRAUD_THRESHOLD = 5000.0


def main():
    transactions = []
    accounts = [f"account_{i:04d}" for i in range(10)]
    for i in range(100):
        transactions.append({
            "id": i,
            "account": random.choice(accounts),
            "amount": round(random.uniform(100, 10000), 2),
            "type": random.choice(["purchase", "withdrawal"]),
        })

    flow = liutang.Flow(name="fraud-detection", mode=liutang.RuntimeMode.BATCH)
    stream = flow.from_collection(transactions)
    flagged = (
        stream
        .filter(lambda tx: tx["amount"] >= FRAUD_THRESHOLD, name="suspicious_amount")
        .map(lambda tx: {**tx, "risk": "HIGH" if tx["amount"] >= 8000 else "MEDIUM"}, name="assess_risk")
    )
    sink = flagged.collect()

    flow.execute()
    print("\n--- Fraud Detection ---")
    print(f"Total flagged: {len(sink.results)}")
    high_risk = [tx for tx in sink.results if isinstance(tx, dict) and tx.get("risk") == "HIGH"]
    print(f"High risk: {len(high_risk)}")
    for tx in sink.results[:5]:
        print(f"  {tx}")


if __name__ == "__main__":
    main()