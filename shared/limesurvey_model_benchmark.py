#!/usr/bin/env python3
"""
LimeSurvey ML Model Benchmark

Compares different embedding models and classifiers to determine
if accuracy improvements are worth the performance cost.

Tests:
- Embedding models: all-MiniLM-L6-v2 (current) vs all-mpnet-base-v2
- Classifiers: RandomForest (current) vs GradientBoosting

Metrics:
- Cross-validation accuracy (5-fold)
- Training time
- Inference time (per 100 samples)
- Model size

Usage:
    # On Linux (with GCP credentials) - export data and run benchmark:
    python shared/limesurvey_model_benchmark.py --export-data
    python shared/limesurvey_model_benchmark.py

    # On Windows (offline) - use cached data:
    python shared/limesurvey_model_benchmark.py --use-cache
"""

import os
import sys
import time
import pickle
import warnings
import argparse
from datetime import datetime
from pathlib import Path

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore', category=DeprecationWarning)
warnings.filterwarnings('ignore', category=FutureWarning)
os.environ['GRPC_VERBOSITY'] = 'ERROR'
os.environ['GLOG_minloglevel'] = '3'

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import cross_val_score, train_test_split
from sentence_transformers import SentenceTransformer

# Configuration
PROJECT_ID = "dashboards-267800"
DATASET = "dashboards"
TABLE = "lime_surveys_columnar_completed"
CACHE_FILE = Path(__file__).parent / "benchmark_training_data.pkl"
RESULTS_FILE = Path(__file__).parent / "benchmark_results.json"

# Sample questions that mimic real LimeSurvey data for demo mode
DEMO_QUESTIONS = {
    "age": [
        "What is your age?", "How old are you?", "Your age:", "Age group:",
        "What age range are you in?", "Please select your age bracket",
        "In which age group do you fall?", "What is your current age?",
        "Age at time of survey", "Your age in years", "How many years old are you?",
        "Select your age category", "What's your age?", "Age:",
    ],
    "gender": [
        "What is your gender?", "Gender:", "Sex:", "Are you male or female?",
        "What sex were you assigned at birth?", "Gender identity:",
        "Please indicate your gender", "Your gender:", "M/F:",
        "What is your biological sex?", "Gender (select one):",
    ],
    "diagnosis": [
        "What is your diagnosis?", "Primary diagnosis:", "Disease type:",
        "What condition were you diagnosed with?", "Your medical diagnosis:",
        "Diagnosis received:", "What disease do you have?",
        "Medical condition:", "What were you diagnosed with?",
    ],
    "treatment": [
        "What treatment are you receiving?", "Current treatment:",
        "Treatment type:", "What medications are you taking?",
        "Therapy received:", "What is your current therapy?",
        "Treatment regimen:", "What treatment are you on?",
    ],
    "symptoms": [
        "What symptoms do you experience?", "Current symptoms:",
        "Describe your symptoms", "What problems are you having?",
        "Symptom severity:", "Rate your symptoms",
    ],
    "quality_of_life": [
        "How would you rate your quality of life?", "Quality of life score:",
        "Rate your overall wellbeing", "How are you feeling overall?",
        "Life satisfaction:", "Overall health rating:",
    ],
    "caregiver": [
        "Do you have a caregiver?", "Who is your primary caregiver?",
        "Caregiver relationship:", "Does someone help care for you?",
    ],
    "employment": [
        "What is your employment status?", "Are you currently employed?",
        "Employment:", "Work status:", "Do you work?",
    ],
    "education": [
        "What is your highest level of education?", "Education level:",
        "Highest degree earned:", "Educational background:",
    ],
    "income": [
        "What is your household income?", "Annual income:",
        "Income bracket:", "Household earnings:",
    ],
}

# Models to test
EMBEDDING_MODELS = {
    'MiniLM (current)': 'all-MiniLM-L6-v2',
    'MPNet (larger)': 'all-mpnet-base-v2',
}

CLASSIFIERS = {
    'RandomForest (current)': lambda: RandomForestClassifier(
        n_estimators=100,
        max_depth=10,
        n_jobs=-1,
        random_state=42
    ),
    'GradientBoosting': lambda: GradientBoostingClassifier(
        n_estimators=100,
        max_depth=5,
        learning_rate=0.1,
        random_state=42
    ),
}


def generate_demo_data(num_samples: int = 500) -> pd.DataFrame:
    """Generate realistic demo training data for benchmarking

    Creates variations of questions with typos, extra words, and rephrasing
    to simulate real survey data variety.
    """
    import random

    questions = []
    labels = []

    for label, base_questions in DEMO_QUESTIONS.items():
        # Use each base question multiple times with variations
        samples_per_label = num_samples // len(DEMO_QUESTIONS)

        for _ in range(samples_per_label):
            base = random.choice(base_questions)

            # Apply random variations
            variation = random.random()
            if variation < 0.3:
                # Add prefix
                prefixes = ["Please answer: ", "Q: ", "Survey question - ", ""]
                q = random.choice(prefixes) + base
            elif variation < 0.5:
                # Add suffix
                suffixes = [" (required)", " (optional)", " - please select", ""]
                q = base + random.choice(suffixes)
            elif variation < 0.7:
                # Lowercase or uppercase
                q = base.lower() if random.random() < 0.5 else base.upper()
            else:
                q = base

            questions.append(q)
            labels.append(label)

    df = pd.DataFrame({
        'raw_question_en': questions,
        'standardized_question': labels
    })

    # Shuffle
    df = df.sample(frac=1, random_state=42).reset_index(drop=True)

    return df


def load_training_data(use_cache: bool = False, export_data: bool = False, demo_mode: bool = False):
    """Load training data from BigQuery, cache, or generate demo data

    Args:
        use_cache: If True, load from local cache file instead of BigQuery
        export_data: If True, save data to cache file after loading from BigQuery
        demo_mode: If True, generate synthetic demo data for testing
    """
    if demo_mode:
        print("Generating demo training data...")
        df = generate_demo_data(500)
        print(f"Generated {len(df):,} demo training examples")
        print(f"Unique labels: {df['standardized_question'].nunique()}")
        return df

    if use_cache:
        if CACHE_FILE.exists():
            print(f"Loading cached training data from {CACHE_FILE}...")
            with open(CACHE_FILE, 'rb') as f:
                df = pickle.load(f)
            print(f"Loaded {len(df):,} training examples from cache")
            print(f"Unique labels: {df['standardized_question'].nunique()}")
            return df
        else:
            print(f"ERROR: Cache file not found: {CACHE_FILE}")
            print("Run with --export-data on a machine with GCP credentials first.")
            sys.exit(1)

    print("Loading training data from BigQuery...")

    try:
        from google.cloud import bigquery
        client = bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        print(f"ERROR: Could not connect to BigQuery: {e}")
        if CACHE_FILE.exists():
            print(f"\nFalling back to cached data...")
            return load_training_data(use_cache=True)
        else:
            print("\nTip: Run with --use-cache to use offline data, or")
            print("     Run with --export-data on Linux to create cache file first.")
            sys.exit(1)

    query = f"""
    SELECT DISTINCT
        raw_question_en,
        standardized_question
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE standardized_question IS NOT NULL
      AND raw_question_en IS NOT NULL
      AND TRIM(raw_question_en) != ''
    """

    df = client.query(query).to_dataframe()
    print(f"Loaded {len(df):,} training examples")
    print(f"Unique labels: {df['standardized_question'].nunique()}")

    if export_data:
        print(f"\nExporting training data to {CACHE_FILE}...")
        with open(CACHE_FILE, 'wb') as f:
            pickle.dump(df, f)
        print(f"Data exported successfully ({CACHE_FILE.stat().st_size:,} bytes)")

    return df


def cleanup_text(text: str) -> str:
    """Basic text cleanup"""
    import re
    import html

    if not text:
        return ""

    # Decode HTML entities
    text = html.unescape(text)

    # Remove HTML tags
    text = re.sub(r'<[^>]+>', ' ', text)

    # Normalize whitespace
    text = ' '.join(text.split())

    return text.strip()


def generate_embeddings(model_name: str, texts: list) -> tuple:
    """Generate embeddings and measure time"""
    print(f"\n  Loading embedding model: {model_name}")

    start_load = time.time()
    model = SentenceTransformer(model_name)
    load_time = time.time() - start_load

    print(f"  Generating embeddings for {len(texts):,} texts...")
    start_embed = time.time()
    embeddings = model.encode(texts, show_progress_bar=True)
    embed_time = time.time() - start_embed

    # Measure inference time on small batch
    sample_texts = texts[:100]
    start_infer = time.time()
    _ = model.encode(sample_texts, show_progress_bar=False)
    infer_time = time.time() - start_infer

    return embeddings, {
        'load_time': load_time,
        'embed_time': embed_time,
        'infer_time_100': infer_time,
        'embedding_dim': embeddings.shape[1],
    }


def train_and_evaluate(clf_name: str, clf_factory, X: np.ndarray, y: np.ndarray) -> dict:
    """Train classifier and evaluate with cross-validation"""
    print(f"\n  Training {clf_name}...")

    # Create classifier
    clf = clf_factory()

    # Cross-validation accuracy (5-fold)
    print(f"  Running 5-fold cross-validation...")
    start_cv = time.time()
    cv_scores = cross_val_score(clf, X, y, cv=5, n_jobs=-1)
    cv_time = time.time() - start_cv

    # Full training for timing and model size
    print(f"  Training on full dataset...")
    start_train = time.time()
    clf.fit(X, y)
    train_time = time.time() - start_train

    # Inference time on 100 samples
    start_infer = time.time()
    _ = clf.predict(X[:100])
    infer_time = time.time() - start_infer

    # Model size
    model_bytes = pickle.dumps(clf)
    model_size = len(model_bytes)

    return {
        'cv_mean': cv_scores.mean(),
        'cv_std': cv_scores.std(),
        'cv_time': cv_time,
        'train_time': train_time,
        'infer_time_100': infer_time,
        'model_size': model_size,
    }


def format_time(seconds: float) -> str:
    """Format time in human-readable format"""
    if seconds < 1:
        return f"{seconds*1000:.0f}ms"
    elif seconds < 60:
        return f"{seconds:.1f}s"
    else:
        mins = int(seconds // 60)
        secs = seconds % 60
        return f"{mins}m {secs:.0f}s"


def format_size(bytes: int) -> str:
    """Format bytes in human-readable format"""
    if bytes < 1024:
        return f"{bytes}B"
    elif bytes < 1024**2:
        return f"{bytes/1024:.1f}KB"
    else:
        return f"{bytes/1024**2:.1f}MB"


def generate_charts(results: dict, output_dir: Path):
    """Generate comparison charts"""
    try:
        import matplotlib.pyplot as plt
        import matplotlib
        matplotlib.use('Agg')  # Non-interactive backend
    except ImportError:
        print("matplotlib not installed - skipping chart generation")
        return None

    # Prepare data
    configs = list(results.keys())
    accuracies = [results[c]['cv_mean'] * 100 for c in configs]
    stds = [results[c]['cv_std'] * 100 for c in configs]
    train_times = [results[c]['train_time'] for c in configs]
    infer_times = [results[c]['infer_time_100'] * 1000 for c in configs]  # Convert to ms

    # Short labels for chart
    short_labels = [c.replace(' (current)', '').replace(' (larger)', '') for c in configs]

    # Create figure with 3 subplots
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))

    # Color scheme
    colors = ['#1a73e8', '#34a853', '#ea4335', '#fbbc04']

    # 1. Accuracy comparison
    ax1 = axes[0]
    bars1 = ax1.bar(range(len(configs)), accuracies, yerr=stds, color=colors, capsize=5)
    ax1.set_ylabel('Accuracy (%)', fontsize=12)
    ax1.set_title('Cross-Validation Accuracy', fontsize=14, fontweight='bold')
    ax1.set_xticks(range(len(configs)))
    ax1.set_xticklabels(short_labels, rotation=45, ha='right', fontsize=9)
    ax1.set_ylim(min(accuracies) - 5, 100)

    # Add value labels on bars
    for bar, acc in zip(bars1, accuracies):
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                f'{acc:.2f}%', ha='center', va='bottom', fontsize=10)

    # 2. Training time comparison
    ax2 = axes[1]
    bars2 = ax2.bar(range(len(configs)), train_times, color=colors)
    ax2.set_ylabel('Training Time (seconds)', fontsize=12)
    ax2.set_title('Training Time', fontsize=14, fontweight='bold')
    ax2.set_xticks(range(len(configs)))
    ax2.set_xticklabels(short_labels, rotation=45, ha='right', fontsize=9)

    for bar, t in zip(bars2, train_times):
        ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1,
                f'{t:.1f}s', ha='center', va='bottom', fontsize=10)

    # 3. Inference time comparison
    ax3 = axes[2]
    bars3 = ax3.bar(range(len(configs)), infer_times, color=colors)
    ax3.set_ylabel('Inference Time (ms per 100 samples)', fontsize=12)
    ax3.set_title('Inference Speed', fontsize=14, fontweight='bold')
    ax3.set_xticks(range(len(configs)))
    ax3.set_xticklabels(short_labels, rotation=45, ha='right', fontsize=9)

    for bar, t in zip(bars3, infer_times):
        ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                f'{t:.0f}ms', ha='center', va='bottom', fontsize=10)

    plt.tight_layout()

    # Save chart
    chart_path = output_dir / "benchmark_comparison.png"
    plt.savefig(chart_path, dpi=150, bbox_inches='tight')
    plt.close()

    print(f"\nChart saved to: {chart_path}")
    return chart_path


def print_results_table(results: dict):
    """Print results in a formatted table"""
    print("\n" + "=" * 100)
    print(" BENCHMARK RESULTS ".center(100, "="))
    print("=" * 100)

    # Header
    print(f"\n{'Configuration':<35} {'Accuracy':<15} {'CV Time':<12} {'Train Time':<12} {'Infer/100':<12} {'Size':<10}")
    print("-" * 100)

    # Sort by accuracy descending
    sorted_results = sorted(results.items(), key=lambda x: x[1]['cv_mean'], reverse=True)

    for config, metrics in sorted_results:
        accuracy = f"{metrics['cv_mean']:.4f} (+/-{metrics['cv_std']:.4f})"
        cv_time = format_time(metrics['cv_time'])
        train_time = format_time(metrics['train_time'])
        infer_time = format_time(metrics['infer_time_100'])
        size = format_size(metrics['model_size'])

        print(f"{config:<35} {accuracy:<15} {cv_time:<12} {train_time:<12} {infer_time:<12} {size:<10}")

    print("-" * 100)

    # Find best and current
    best_config = sorted_results[0][0]
    best_acc = sorted_results[0][1]['cv_mean']

    current_config = "MiniLM (current) + RandomForest (current)"
    current_acc = results.get(current_config, {}).get('cv_mean', 0)

    print(f"\n{'ANALYSIS':=^100}")
    print(f"\nCurrent configuration: {current_config}")
    print(f"Current accuracy: {current_acc:.4f} ({current_acc*100:.2f}%)")
    print(f"\nBest configuration: {best_config}")
    print(f"Best accuracy: {best_acc:.4f} ({best_acc*100:.2f}%)")

    if best_config != current_config:
        improvement = best_acc - current_acc
        print(f"\nPotential improvement: {improvement:+.4f} ({improvement*100:+.2f}%)")

        # Compare performance costs
        current_metrics = results[current_config]
        best_metrics = results[best_config]

        print(f"\nPerformance tradeoff:")
        print(f"  Training time: {format_time(current_metrics['train_time'])} -> {format_time(best_metrics['train_time'])} ({best_metrics['train_time']/current_metrics['train_time']:.1f}x)")
        print(f"  Inference time: {format_time(current_metrics['infer_time_100'])} -> {format_time(best_metrics['infer_time_100'])} ({best_metrics['infer_time_100']/current_metrics['infer_time_100']:.1f}x)")
        print(f"  Model size: {format_size(current_metrics['model_size'])} -> {format_size(best_metrics['model_size'])}")
    else:
        print(f"\nCurrent configuration is already optimal!")

    print("\n" + "=" * 100)


def main():
    parser = argparse.ArgumentParser(description='LimeSurvey ML Model Benchmark')
    parser.add_argument('--use-cache', action='store_true',
                        help='Use cached training data (for offline testing)')
    parser.add_argument('--export-data', action='store_true',
                        help='Export training data to cache file for offline use')
    parser.add_argument('--demo', action='store_true',
                        help='Use synthetic demo data (no GCP credentials needed)')
    args = parser.parse_args()

    print("=" * 70)
    print(" LIMESURVEY ML MODEL BENCHMARK ".center(70))
    print("=" * 70)
    print(f"\nStarted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Load data
    df = load_training_data(use_cache=args.use_cache, export_data=args.export_data, demo_mode=args.demo)

    if args.export_data:
        print("\nData exported. Run without --export-data to run the benchmark.")
        return {}

    # Clean texts
    print("\nCleaning text data...")
    questions = [cleanup_text(q) for q in df['raw_question_en']]
    labels = df['standardized_question'].tolist()

    # Filter empty
    valid = [(q, l) for q, l in zip(questions, labels) if q]
    questions = [q for q, _ in valid]
    labels = [l for _, l in valid]

    print(f"Valid examples after cleanup: {len(questions):,}")

    # Encode labels
    label_encoder = LabelEncoder()
    encoded_labels = label_encoder.fit_transform(labels)
    print(f"Unique classes: {len(label_encoder.classes_)}")

    # Store all results
    all_results = {}
    embedding_cache = {}

    # Test each embedding model
    for embed_name, embed_model in EMBEDDING_MODELS.items():
        print(f"\n{'='*70}")
        print(f"TESTING EMBEDDING MODEL: {embed_name}")
        print(f"{'='*70}")

        # Generate embeddings (cache for reuse)
        embeddings, embed_metrics = generate_embeddings(embed_model, questions)
        embedding_cache[embed_name] = embeddings

        print(f"\n  Embedding dimensions: {embed_metrics['embedding_dim']}")
        print(f"  Model load time: {format_time(embed_metrics['load_time'])}")
        print(f"  Full embedding time: {format_time(embed_metrics['embed_time'])}")
        print(f"  Inference time (100 samples): {format_time(embed_metrics['infer_time_100'])}")

        # Test each classifier
        for clf_name, clf_factory in CLASSIFIERS.items():
            config_name = f"{embed_name} + {clf_name}"
            print(f"\n  --- {clf_name} ---")

            clf_metrics = train_and_evaluate(clf_name, clf_factory, embeddings, encoded_labels)

            # Combine metrics
            all_results[config_name] = {
                **clf_metrics,
                'embed_time': embed_metrics['embed_time'],
                'embedding_dim': embed_metrics['embedding_dim'],
            }

            print(f"  CV Accuracy: {clf_metrics['cv_mean']:.4f} (+/- {clf_metrics['cv_std']:.4f})")
            print(f"  Training time: {format_time(clf_metrics['train_time'])}")
            print(f"  Inference time (100 samples): {format_time(clf_metrics['infer_time_100'])}")

    # Print final comparison table
    print_results_table(all_results)

    # Generate charts
    output_dir = Path(__file__).parent
    chart_path = generate_charts(all_results, output_dir)

    # Save results to JSON
    import json
    results_json = {k: {kk: float(vv) if isinstance(vv, (np.floating, float)) else vv
                        for kk, vv in v.items()}
                    for k, v in all_results.items()}
    with open(RESULTS_FILE, 'w') as f:
        json.dump(results_json, f, indent=2)
    print(f"Results saved to: {RESULTS_FILE}")

    print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    return all_results


if __name__ == "__main__":
    main()
