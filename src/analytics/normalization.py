import re

class DataNormalizer:
    @staticmethod
    def harmonize_category(text):
        """
        Maps various raw scrape categories into unified buckets.
        """
        text = str(text).lower()
        if any(kw in text for kw in ['bag', 'sac', 'pochette', 'tote']):
            return "Bags"
        if any(kw in text for kw in ['shoe', 'soulier', 'mule', 'sandale', 'escarpin', 'chaussure']):
            return "Shoes"
        if any(kw in text for kw in ['jewelry', 'bijou', 'collier', 'bracelet', 'bague']):
            return "Jewelry"
        if any(kw in text for kw in ['ready', 'pret', 'blouse', 'robe', 't-shirt', 'veste', 'chemise']):
            return "Ready-to-Wear"
        return "Other"

    @staticmethod
    def clean_text(text):
        """
        Removes brand names, colors, and noise for better fuzzy matching.
        """
        if not text: return ""
        text = str(text).lower()
        # Remove brand names
        text = re.sub(r'dior|christian|rebag|vestiaire', '', text)
        # Remove common colors/attributes
        text = re.sub(r'black|noir|blue|bleu|pink|rose|white|blanc|red|rouge|medium|small|petit|grand', '', text)
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    @staticmethod
    def extract_numeric_price(price_str):
        """
        Converts string prices (e.g., '4 100,00 €') to float.
        """
        if not price_str or price_str == "N/A": return 0.0
        # Remove currency symbols and spaces
        clean_price = re.sub(r'[^\d,.]', '', str(price_str))
        # Handle European decimal comma
        if ',' in clean_price and '.' in clean_price:
            clean_price = clean_price.replace(',', '') # thousands sep
        elif ',' in clean_price:
            clean_price = clean_price.replace(',', '.')
            
        try:
            return float(clean_price)
        except:
            return 0.0
