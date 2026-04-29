"""
ML Classifier Inference Service for OrcaFlow

Loads trained model and provides classification predictions for job submissions.
"""

import os
import json
import joblib
import logging
from typing import Tuple, Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class JobClassifier:
    """Inference service for job classification"""
    
    def __init__(self, model_path: str = "models/job_classifier.pkl"):
        self.model_path = model_path
        self.model = None
        self.scaler = None
        self.config = None
        self.job_classes = ['small_quick', 'medium_cpu', 'large_intensive']
        
        self.load_model()
    
    def load_model(self):
        """Load trained model and scaler from disk"""
        try:
            # Load model
            if not os.path.exists(self.model_path):
                logger.warning(f"Model not found at {self.model_path}, using dummy predictions")
                self.model = None
                return
            
            self.model = joblib.load(self.model_path)
            logger.info(f"✅ Loaded model from {self.model_path}")
            
            # Load scaler
            scaler_path = self.model_path.replace('.pkl', '_scaler.pkl')
            self.scaler = joblib.load(scaler_path)
            logger.info(f"✅ Loaded scaler from {scaler_path}")
            
            # Load config
            config_path = self.model_path.replace('.pkl', '_config.json')
            with open(config_path, 'r') as f:
                self.config = json.load(f)
            logger.info(f"✅ Loaded config from {config_path}")
            
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            self.model = None
    
    def classify(self, features: Dict[str, float]) -> Dict[str, Any]:
        """
        Classify a job based on its features.
        
        Args:
            features: {
                'dataset_size_mb': float,
                'code_complexity_score': float (0-10),
                'memory_requirement_mb': float,
                'cpu_requirement_cores': float
            }
        
        Returns:
            {
                'job_class': str,
                'predicted_class_id': int,
                'confidence': float,
                'class_probabilities': dict
            }
        """
        
        # Extract features in order
        feature_vector = [
            features.get('dataset_size_mb', 100),
            features.get('code_complexity_score', 5),
            features.get('memory_requirement_mb', 512),
            features.get('cpu_requirement_cores', 1)
        ]
        
        if self.model is None:
            # Fallback to rule-based classification
            return self._rule_based_classify(feature_vector)
        
        try:
            # Scale features
            X_scaled = self.scaler.transform([feature_vector])
            
            # Get prediction
            prediction = self.model.predict(X_scaled)[0]
            probabilities = self.model.predict_proba(X_scaled)[0]
            confidence = float(max(probabilities))
            
            return {
                'job_class': self.job_classes[prediction],
                'predicted_class_id': int(prediction),
                'confidence': confidence,
                'class_probabilities': {
                    self.job_classes[i]: float(probabilities[i])
                    for i in range(len(self.job_classes))
                }
            }
        
        except Exception as e:
            logger.error(f"Classification error: {e}, falling back to rules")
            return self._rule_based_classify(feature_vector)
    
    def _rule_based_classify(self, feature_vector: list) -> Dict[str, Any]:
        """
        Fallback rule-based classification when ML model unavailable.
        
        Uses simple heuristics based on feature values.
        """
        
        dataset_size = feature_vector[0]
        complexity = feature_vector[1]
        memory = feature_vector[2]
        cpu = feature_vector[3]
        
        # Compute composite score
        score = (
            (dataset_size / 1000) * 0.4 +
            (complexity / 10) * 0.2 +
            (cpu / 16) * 0.3 +
            (memory / 8192) * 0.1
        )
        
        if score < 0.3:
            predicted_class = 0
        elif score < 0.7:
            predicted_class = 1
        else:
            predicted_class = 2
        
        return {
            'job_class': self.job_classes[predicted_class],
            'predicted_class_id': predicted_class,
            'confidence': 0.5,  # Lower confidence for rule-based
            'class_probabilities': {
                self.job_classes[i]: (0.1 if i != predicted_class else 0.7)
                for i in range(len(self.job_classes))
            }
        }
    
    def get_resource_estimate(self, job_class: str) -> Dict[str, Any]:
        """
        Get estimated resource requirements for a job class.
        
        Based on historical data and classification.
        """
        
        estimates = {
            'small_quick': {
                'estimated_duration_sec': 60,
                'recommended_workers': 1,
                'cpu_cores': 1,
                'memory_mb': 512,
                'estimated_cost_usd': 0.01
            },
            'medium_cpu': {
                'estimated_duration_sec': 600,
                'recommended_workers': 2,
                'cpu_cores': 4,
                'memory_mb': 2048,
                'estimated_cost_usd': 0.10
            },
            'large_intensive': {
                'estimated_duration_sec': 3600,
                'recommended_workers': 4,
                'cpu_cores': 16,
                'memory_mb': 8192,
                'estimated_cost_usd': 0.40
            }
        }
        
        return estimates.get(job_class, estimates['medium_cpu'])


def main():
    """Demo of classifier inference"""
    logger.info("🚀 OrcaFlow Job Classifier Inference Service")
    
    # Initialize classifier
    classifier = JobClassifier()
    
    # Test classifications
    test_jobs = [
        {
            'name': 'Small ML Job',
            'dataset_size_mb': 50,
            'code_complexity_score': 2,
            'memory_requirement_mb': 256,
            'cpu_requirement_cores': 1
        },
        {
            'name': 'Medium Data Processing',
            'dataset_size_mb': 1000,
            'code_complexity_score': 5,
            'memory_requirement_mb': 2048,
            'cpu_requirement_cores': 4
        },
        {
            'name': 'Large Training Job',
            'dataset_size_mb': 15000,
            'code_complexity_score': 8,
            'memory_requirement_mb': 16384,
            'cpu_requirement_cores': 16
        }
    ]
    
    logger.info("\n📊 Classification Results:\n")
    
    for job in test_jobs:
        result = classifier.classify(job)
        resources = classifier.get_resource_estimate(result['job_class'])
        
        logger.info(f"Job: {job['name']}")
        logger.info(f"  Classification: {result['job_class']} (confidence: {result['confidence']:.2%})")
        logger.info(f"  Estimated Duration: {resources['estimated_duration_sec']}s")
        logger.info(f"  Recommended Workers: {resources['recommended_workers']}")
        logger.info(f"  Estimated Cost: ${resources['estimated_cost_usd']:.2f}")
        logger.info("")


if __name__ == "__main__":
    main()
