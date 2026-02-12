from difflib import SequenceMatcher
import pandas as pd

class ValueAnalyzer:
    def __init__(self, similarity_threshold=0.7):
        self.threshold = similarity_threshold

    @staticmethod
    def calculate_similarity(a, b):
        return SequenceMatcher(None, a, b).ratio()

    def match_listings(self, retail_df, resale_df):
        """
        Matches resale listings to retail products using fuzzy name similarity.
        """
        matches = []
        
        for idx, resale in resale_df.iterrows():
            best_match = None
            max_sim = 0
            
            # Simple optimization: only match within same category
            potential_retail = retail_df[retail_df['category'] == resale['category']]
            
            for _, retail in potential_retail.iterrows():
                sim = self.calculate_similarity(resale['product_name_clean'], retail['product_name_clean'])
                if sim > max_sim and sim >= self.threshold:
                    max_sim = sim
                    best_match = retail
            
            if best_match is not None:
                matches.append({
                    'product_name': best_match['product_name'],
                    'category': best_match['category'],
                    'retail_price_eur': retail['retail_price_num'], # Assume EUR for now
                    'resale_price_eur': resale['resale_price_num'],
                    'similarity': max_sim,
                    'condition': resale['Condition'],
                    'source': resale['Source'],
                    'availability_status': best_match['availability'],
                    'scrape_date': resale['scrape_date']
                })
        
        return pd.DataFrame(matches)

    @staticmethod
    def calculate_metrics(df):
        """
        Calculates RVR (Resale Value Retention) and Classifies.
        """
        if df.empty: return df
        
        # Avoid division by zero
        df = df[df['retail_price_eur'] > 0].copy()
        
        df['RVR'] = df['resale_price_eur'] / df['retail_price_eur']
        
        def classify_rvr(rvr):
            if rvr >= 1.0: return "Investment-like"
            if rvr >= 0.9: return "Value-retaining"
            return "Depreciating"
            
        df['value_class'] = df['RVR'].apply(classify_rvr)
        return df
