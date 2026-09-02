# Purpose: Train a risk scoring model on synthetic data and export to ONNX.
#
# Three approaches offered (run with --approach 1, 2, or 3):
#
#   1  Rule engine only (no training) — Python translation of the Flink SQL rules.
#      Use as a baseline and to generate training labels.
#
#   2  RandomForestClassifier → ONNX  (~10 seconds)
#      Trains on 10 000 synthetic examples labelled by the rule engine.
#      Export with skl2onnx so onnxruntime handles inference — no sklearn at serve time.
#
#   3  DecisionTreeClassifier → ONNX  (even faster, human-readable)
#      Same pipeline but with a shallow decision tree (max_depth=4).
#      Call with --print-tree to see the learned rules — compare to the hand-written
#      SQL rules as a demo talking point: "the model rediscovered the same logic."
#
# Output: model/risk_model.onnx  (used by ../app.py)

import argparse
import os
import random

import numpy as np

# Labels — must match RISK_LABELS list in app.py
LABELS = ["NORMAL", "RG_ALERT", "SHARP_BETTOR", "STAKE_LIMIT", "FRAUD_HIGH"]
LABEL_TO_INT = {l: i for i, l in enumerate(LABELS)}

FEATURE_NAMES = [
    "bets_last_10min",
    "total_stake_30min",
    "live_bet_ratio_15min",
    "avg_odds_15min",
    "current_loss_streak",
    "geo_mismatches_15min",
    "distinct_sports_30min",
    "net_pnl_30min",
]


# ── Rule engine (approach 1) ───────────────────────────────────────────────────
def rule_engine(f: dict) -> str:
    """Python translation of 04_risk_scoring_inference.sql rules."""
    if f["bets_last_10min"] > 8 or f["geo_mismatches_15min"] > 1:
        return "FRAUD_HIGH"
    if (f["current_loss_streak"] >= 6
            or f["live_bet_ratio_15min"] > 0.5
            or f["distinct_sports_30min"] > 8):
        return "RG_ALERT"
    if f["avg_odds_15min"] < 2 and f["bets_last_10min"] > 5:
        return "SHARP_BETTOR"
    if f["total_stake_30min"] > 500:
        return "STAKE_LIMIT"
    return "NORMAL"


# ── Synthetic dataset generator ────────────────────────────────────────────────
def generate_dataset(n: int = 10000, seed: int = 42):
    rng = random.Random(seed)
    X, y = [], []

    for _ in range(n):
        f = {
            "bets_last_10min":      rng.choices([rng.randint(0, 6), rng.randint(9, 30)],    weights=[0.88, 0.12])[0],
            "total_stake_30min":    rng.choices([rng.uniform(0, 300), rng.uniform(400, 1500)], weights=[0.90, 0.10])[0],
            "live_bet_ratio_15min": rng.uniform(0.0, 1.0),
            "avg_odds_15min":       rng.choices([rng.uniform(2.0, 6.0), rng.uniform(1.2, 1.99)], weights=[0.90, 0.10])[0],
            "current_loss_streak":  rng.choices([rng.randint(0, 4), rng.randint(6, 15)],    weights=[0.90, 0.10])[0],
            "geo_mismatches_15min": rng.choices([0, rng.randint(2, 5)],                      weights=[0.96, 0.04])[0],
            "distinct_sports_30min":rng.choices([rng.randint(1, 7), rng.randint(9, 11)],    weights=[0.82, 0.18])[0],
            "net_pnl_30min":        rng.uniform(-500, 150),
        }
        label = rule_engine(f)
        X.append([f[k] for k in FEATURE_NAMES])
        y.append(LABEL_TO_INT[label])

    return np.array(X, dtype=np.float32), np.array(y, dtype=np.int64)


# ── Approach 2 & 3: sklearn → ONNX ────────────────────────────────────────────
def train_and_export(approach: int, print_tree: bool, output_path: str):
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.tree import DecisionTreeClassifier, export_text
    from skl2onnx import convert_sklearn
    from skl2onnx.common.data_types import FloatTensorType

    print(f"Generating synthetic training dataset (10 000 examples)...")
    X, y = generate_dataset(n=10000)
    label_counts = {LABELS[i]: int(np.sum(y == i)) for i in range(len(LABELS))}
    print(f"  Label distribution: {label_counts}")

    if approach == 2:
        print("Training RandomForestClassifier (n_estimators=100, max_depth=6)...")
        clf = RandomForestClassifier(n_estimators=100, max_depth=6, random_state=42, n_jobs=-1)
    else:
        print("Training DecisionTreeClassifier (max_depth=4)...")
        clf = DecisionTreeClassifier(max_depth=4, random_state=42)

    clf.fit(X, y)
    acc = clf.score(X, y)
    print(f"  Training accuracy: {acc:.3f}  (silver labels from rule engine — expected ~0.99)")

    if print_tree and approach == 3:
        print("\nLearned decision tree (compare to hand-written SQL rules):\n")
        print(export_text(clf, feature_names=FEATURE_NAMES))

    print(f"\nExporting to ONNX: {output_path}")
    initial_type = [("features", FloatTensorType([None, len(FEATURE_NAMES)]))]
    onnx_model = convert_sklearn(clf, initial_types=initial_type,
                                 target_opset={"": 17, "ai.onnx.ml": 3})
    with open(output_path, "wb") as f:
        f.write(onnx_model.SerializeToString())
    print(f"  Saved. Model size: {os.path.getsize(output_path) / 1024:.1f} KB")
    print(f"\nDone. Load in app.py via: ort_session = rt.InferenceSession('{output_path}')")


# ── Main ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train betting risk model")
    parser.add_argument("--approach", type=int, default=2, choices=[1, 2, 3],
                        help="1=rule engine only, 2=RandomForest→ONNX, 3=DecisionTree→ONNX")
    parser.add_argument("--print-tree", action="store_true",
                        help="(approach 3 only) Print the learned decision tree")
    parser.add_argument("--output", default=os.path.join(os.path.dirname(__file__), "risk_model.onnx"),
                        help="Output path for the ONNX model file")
    args = parser.parse_args()

    if args.approach == 1:
        print("Approach 1: Rule engine only (no model file generated).")
        print("The rule engine in app.py is the fallback when no ONNX model is present.")
        print("\nExample prediction:")
        test = {"bets_last_10min": 12, "total_stake_30min": 200, "live_bet_ratio_15min": 0.3,
                "avg_odds_15min": 2.5, "current_loss_streak": 2, "geo_mismatches_15min": 2,
                "distinct_sports_30min": 3, "net_pnl_30min": -50}
        print(f"  Input:  {test}")
        print(f"  Output: {rule_engine(test)}")
    else:
        train_and_export(args.approach, args.print_tree, args.output)
