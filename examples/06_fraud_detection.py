"""
Example: Fraud Detection Pipeline (inspired by PyFlink-Tutorial)

Transaction data stream with threshold-based filtering and alerting.
Uses the local engine with callback sink for real-time alerts.
"""
import liutang
import random


FRAUD_THRESHOLD = 5000.0


def generate_transactions(n: int = 20) -> list:
    names = [f"account_{i:04d}" for i in range(10)]
    transactions = []
    for i in range(n):
        amount = random.uniform(100, 10000)
        transactions.append({
            "id": i,
            "account": random.choice(names),
            "amount": round(amount, 2),
            "type": "purchase" if random.random() > 0.1 else "withdrawal",
        })
    return transactions


def main():
    transactions = generate_transactions(100)

    flow = liutang.Flow(name="fraud-detection", engine="local", mode=liutang.RuntimeMode.BATCH)
    flow.set_parallelism(4)

    stream = flow.from_collection(transactions)

    flagged = (
        stream
        .filter(lambda tx: tx["amount"] >= FRAUD_THRESHOLD, name="suspicious_amount")
        .map(lambda tx: {**tx, "risk": "HIGH" if tx["amount"] >= 8000 else "MEDIUM"}, name="assess_risk")
    )
    flagged.sink_to(liutang.CallbackSink(
        func=lambda tx: print(f"⚠ SUSPICIOUS: account={tx['account']} amount={tx['amount']} risk={tx['risk']}")
    ))

    results = flow.execute()

    print("\n--- Fraud Detection Summary ---")
    suspicious = results.get("source_0", [])
    print(f"Total flagged: {len(suspicious)}")
    high_risk = [tx for tx in suspicious if isinstance(tx, dict) and tx.get("risk") == "HIGH"]
    print(f"High risk: {len(high_risk)}")


if __name__ == "__main__":
    main()