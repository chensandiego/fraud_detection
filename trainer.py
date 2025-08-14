# trainer.py
import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier

# Build synthetic training set: features: [amount, tx_count_last_1h, avg_amount_last_1h, location_risk]
# label: 1 = fraud, 0 = legit
def make_row(amount, tx_count, avg_amount, loc_risk, fraud):
    return [amount, tx_count, avg_amount, loc_risk, fraud]

rows = [
    # legit examples
    make_row(20, 1, 20, 0, 0),
    make_row(15, 3, 12, 0, 0),
    make_row(200, 1, 200, 0, 0),
]
# add fraudy
for i in range(100):
    rows.append(make_row(np.random.uniform(200, 5000), np.random.randint(1,6), np.random.uniform(100,1200), 1, 1))
for i in range(300):
    rows.append(make_row(np.random.uniform(1,150), np.random.randint(1,5), np.random.uniform(5,80), 0, 0))

df = pd.DataFrame(rows, columns=["amount", "tx_count_1h", "avg_amount_1h", "loc_risk", "label"])
X = df[["amount", "tx_count_1h", "avg_amount_1h", "loc_risk"]]
y = df["label"]

clf = RandomForestClassifier(n_estimators=50, random_state=42)
clf.fit(X, y)
joblib.dump(clf, "model.joblib")
print("Saved model.joblib")
