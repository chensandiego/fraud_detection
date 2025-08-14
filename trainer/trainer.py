import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier

# synthetic dataset
rows = []
for i in range(400):
    amt = np.random.uniform(1, 150)
    rows.append([amt, np.random.randint(1,4), amt, 0, 0])
for i in range(120):
    amt = np.random.uniform(200, 5000)
    rows.append([amt, np.random.randint(1,6), np.random.uniform(100,1200), 1, 1])

df = pd.DataFrame(rows, columns=["amount", "tx_count_1h", "avg_amount_1h", "loc_risk", "label"])
X = df[["amount","tx_count_1h","avg_amount_1h","loc_risk"]]
y = df["label"]
clf = RandomForestClassifier(n_estimators=50, random_state=42)
clf.fit(X,y)
joblib.dump(clf, "model.joblib")
print("Saved model.joblib")