"""
ML Classifier Training Script for OrcaFlow

Trains a model to predict job resource requirements from job metadata.
"""

import os
import json
import pickle
import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class JobClassifierTrainer:
    """Trainer for job classification model"""
    
    def __init__(self):
        self.model = None
        self.scaler = StandardScaler()
        self.feature_names = [
            'dataset_size_mb',
            'code_complexity_score',
            'memory_requirement_mb',
            'cpu_requirement_cores'
        ]
        self.target_name = 'job_class'
    
    def generate_synthetic_data(self, n_samples: int = 1000) -> pd.DataFrame:
        """
        Generate synthetic training data for job classification.
        
        In production, this would use real historical job data.
        """
        logger.info(f"Generating {n_samples} synthetic job records...")
        
        data = {
            'dataset_size_mb': np.random.exponential(500, n_samples),  # Heavy right skew
            'code_complexity_score': np.random.normal(5, 2, n_samples),  # Center around 5
            'memory_requirement_mb': np.random.exponential(1024, n_samples),
            'cpu_requirement_cores': np.random.randint(1, 16, n_samples),
        }
        
        df = pd.DataFrame(data)
        
        # Classify jobs based on features
        # Class 0: Small, simple jobs
        # Class 1: Medium jobs
        # Class 2: Large, complex jobs
        
        def classify_job(row):
            complexity = (
                (row['dataset_size_mb'] / 1000) * 0.5 +
                (row['code_complexity_score'] / 10) * 0.2 +
                (row['cpu_requirement_cores'] / 16) * 0.3
            )
            
            if complexity < 0.3:
                return 0  # Small
            elif complexity < 0.7:
                return 1  # Medium
            else:
                return 2  # Large
        
        df[self.target_name] = df.apply(classify_job, axis=1)
        
        logger.info(f"Data shape: {df.shape}")
        logger.info(f"Class distribution:\n{df[self.target_name].value_counts().sort_index()}")
        
        return df
    
    def train(self, df: pd.DataFrame, test_size: float = 0.2, random_state: int = 42):
        """Train the job classification model"""
        logger.info("Training job classifier...")
        
        X = df[self.feature_names]
        y = df[self.target_name]
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state, stratify=y
        )
        
        # Scale features
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        # Train Random Forest classifier
        self.model = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            min_samples_split=5,
            min_samples_leaf=2,
            random_state=random_state,
            n_jobs=-1
        )
        
        self.model.fit(X_train_scaled, y_train)
        
        # Evaluate
        train_score = self.model.score(X_train_scaled, y_train)
        test_score = self.model.score(X_test_scaled, y_test)
        
        logger.info(f"✅ Model trained!")
        logger.info(f"Train accuracy: {train_score:.4f}")
        logger.info(f"Test accuracy: {test_score:.4f}")
        
        # Detailed metrics
        y_pred = self.model.predict(X_test_scaled)
        logger.info("\nClassification Report:\n" + classification_report(y_test, y_pred))
        
        return test_score
    
    def save(self, model_path: str = "models/job_classifier.pkl"):
        """Save model and scaler to disk"""
        os.makedirs(os.path.dirname(model_path), exist_ok=True)
        
        # Save model
        joblib.dump(self.model, model_path)
        logger.info(f"✅ Model saved to {model_path}")
        
        # Save scaler
        scaler_path = model_path.replace('.pkl', '_scaler.pkl')
        joblib.dump(self.scaler, scaler_path)
        logger.info(f"✅ Scaler saved to {scaler_path}")
        
        # Save feature names for inference
        config = {
            'feature_names': self.feature_names,
            'target_name': self.target_name,
            'job_classes': ['small_quick', 'medium_cpu', 'large_intensive']
        }
        config_path = model_path.replace('.pkl', '_config.json')
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
        logger.info(f"✅ Config saved to {config_path}")
    
    def predict(self, features: list) -> tuple:
        """
        Make a prediction for a single job.
        
        Returns: (predicted_class, confidence_score)
        """
        if self.model is None:
            raise ValueError("Model not trained yet!")
        
        X_scaled = self.scaler.transform([features])
        prediction = self.model.predict(X_scaled)[0]
        confidence = np.max(self.model.predict_proba(X_scaled))
        
        return prediction, confidence


def main():
    """Main training pipeline"""
    logger.info("🚀 Starting OrcaFlow Job Classifier Training...")
    
    # Initialize trainer
    trainer = JobClassifierTrainer()
    
    # Generate synthetic data
    df = trainer.generate_synthetic_data(n_samples=1000)
    
    # Train model
    accuracy = trainer.train(df)
    
    # Save model
    trainer.save()
    
    logger.info(f"✅ Training complete! Final test accuracy: {accuracy:.4f}")
    
    # Demo prediction
    logger.info("\n📊 Demo Predictions:")
    demo_jobs = [
        [100, 3, 512, 1],      # Small job
        [1000, 5, 2048, 4],    # Medium job
        [10000, 8, 8192, 16],  # Large job
    ]
    
    for features in demo_jobs:
        pred, confidence = trainer.predict(features)
        classes = ['small_quick', 'medium_cpu', 'large_intensive']
        logger.info(f"Features: {features} → {classes[pred]} (confidence: {confidence:.2%})")


if __name__ == "__main__":
    main()
