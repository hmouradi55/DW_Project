# nlp_analysis.py
"""
Sentiment analysis and topic modeling for reviews
"""
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import nltk
from textblob import TextBlob
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
import gensim
from gensim import corpora
import logging
import json

# Download required NLTK data
nltk.download('stopwords')
nltk.download('punkt')
nltk.download('punkt_tab')
nltk.download('wordnet')

from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer

logger = logging.getLogger(__name__)

class ReviewAnalyzer:
    def __init__(self, db_config):
        self.engine = create_engine(
            f"postgresql://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )
        self.stop_words = set(stopwords.words('english') + stopwords.words('french'))
        self.lemmatizer = WordNetLemmatizer()
        
    def analyze_sentiment(self):
        """Perform sentiment analysis on reviews"""
        logger.info("Starting sentiment analysis...")
        
        # Load reviews
        query = """
        SELECT id, review_text, rating 
        FROM staging.stg_reviews 
        WHERE review_text IS NOT NULL AND LENGTH(review_text) > 10
        """
        df = pd.read_sql(query, self.engine)
        
        # Analyze sentiment
        sentiments = []
        for idx, row in df.iterrows():
            try:
                blob = TextBlob(row['review_text'])
                polarity = blob.sentiment.polarity
                subjectivity = blob.sentiment.subjectivity
                
                # Classify sentiment
                if polarity > 0.1:
                    sentiment_label = 'positive'
                elif polarity < -0.1:
                    sentiment_label = 'negative'
                else:
                    sentiment_label = 'neutral'
                
                # Consider rating as well
                if row['rating'] >= 4 and sentiment_label == 'neutral':
                    sentiment_label = 'positive'
                elif row['rating'] <= 2 and sentiment_label == 'neutral':
                    sentiment_label = 'negative'
                
                sentiments.append({
                    'review_id': row['id'],
                    'sentiment_label': sentiment_label,
                    'polarity_score': polarity,
                    'subjectivity_score': subjectivity
                })
                
            except Exception as e:
                logger.warning(f"Error analyzing review {row['id']}: {str(e)}")
                sentiments.append({
                    'review_id': row['id'],
                    'sentiment_label': 'neutral',
                    'polarity_score': 0,
                    'subjectivity_score': 0
                })
        
        # Save to database
        sentiment_df = pd.DataFrame(sentiments)
        
        # Create sentiment analysis table
        with self.engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS analytics.sentiment_analysis (
                    review_id INTEGER PRIMARY KEY,
                    sentiment_label VARCHAR(20),
                    polarity_score FLOAT,
                    subjectivity_score FLOAT,
                    analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            conn.commit()
        
        sentiment_df.to_sql('sentiment_analysis', self.engine, schema='analytics',
                          if_exists='replace', index=False)
        
        logger.info(f"Sentiment analysis completed for {len(sentiment_df)} reviews")
        
        # Log summary
        summary = sentiment_df['sentiment_label'].value_counts()
        logger.info(f"Sentiment distribution:\n{summary}")
        
        return sentiment_df
    
    def extract_topics(self, n_topics=10, sentiment_filter=None):
        """Extract topics using LDA"""
        logger.info(f"Starting topic extraction (n_topics={n_topics})...")
        
        # Load reviews with sentiment if available
        query = """
        SELECT r.id, r.review_text, r.rating, r.bank_name,
               COALESCE(s.sentiment_label, 'neutral') as sentiment
        FROM staging.stg_reviews r
        LEFT JOIN analytics.sentiment_analysis s ON r.id = s.review_id
        WHERE r.review_text IS NOT NULL AND LENGTH(r.review_text) > 20
        """
        
        if sentiment_filter:
            query += f" AND s.sentiment_label = '{sentiment_filter}'"
            
        df = pd.read_sql(query, self.engine)
        
        # Preprocess text
        def preprocess_text(text):
            # Tokenize
            tokens = word_tokenize(text.lower())
            # Remove stopwords and short words
            tokens = [t for t in tokens if t not in self.stop_words and len(t) > 3]
            # Lemmatize
            tokens = [self.lemmatizer.lemmatize(t) for t in tokens]
            return ' '.join(tokens)
        
        df['processed_text'] = df['review_text'].apply(preprocess_text)
        
        # Create document-term matrix
        vectorizer = CountVectorizer(max_features=100, ngram_range=(1, 2))
        doc_term_matrix = vectorizer.fit_transform(df['processed_text'])
        
        # LDA model
        lda = LatentDirichletAllocation(
            n_components=n_topics,
            max_iter=50,
            learning_method='online',
            random_state=42
        )
        lda.fit(doc_term_matrix)
        
        # Extract topics
        feature_names = vectorizer.get_feature_names_out()
        topics = []
        
        for topic_idx, topic in enumerate(lda.components_):
            top_indices = topic.argsort()[-10:][::-1]
            top_words = [feature_names[i] for i in top_indices]
            top_weights = [topic[i] for i in top_indices]
            
            topics.append({
                'topic_id': topic_idx,
                'sentiment_filter': sentiment_filter or 'all',
                'top_words': ', '.join(top_words),
                'word_weights': json.dumps(dict(zip(top_words, 
                                                  [float(w) for w in top_weights])))
            })
            
            logger.info(f"Topic {topic_idx}: {', '.join(top_words[:5])}")
        
        # Save topics
        topics_df = pd.DataFrame(topics)
        
        # Create topics table
        with self.engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS analytics.topics (
                    id SERIAL PRIMARY KEY,
                    topic_id INTEGER,
                    sentiment_filter VARCHAR(20),
                    top_words TEXT,
                    word_weights JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            conn.commit()
        
        topics_df.to_sql('topics', self.engine, schema='analytics',
                        if_exists='append', index=False)
        
        # Assign topics to documents
        doc_topics = lda.transform(doc_term_matrix)
        df['primary_topic'] = doc_topics.argmax(axis=1)
        df['topic_score'] = doc_topics.max(axis=1)
        
        # Save document-topic associations
        doc_topic_df = df[['id', 'primary_topic', 'topic_score']]
        
        with self.engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS analytics.review_topics (
                    review_id INTEGER PRIMARY KEY,
                    primary_topic INTEGER,
                    topic_score FLOAT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            conn.commit()
        
        doc_topic_df.rename(columns={'id': 'review_id'}).to_sql(
            'review_topics', self.engine, schema='analytics',
            if_exists='replace', index=False
        )
        
        logger.info(f"Topic extraction completed: {n_topics} topics identified")
        
        return topics_df, df

def main():
    """Run NLP analysis pipeline"""
    db_config = {
        'host': 'localhost',
        'database': 'bank_reviews_dw',
        'user': 'dw_user',
        'password': 'your_secure_password',
        'port': 5432
    }
    
    analyzer = ReviewAnalyzer(db_config)
    
    # Run sentiment analysis
    sentiment_df = analyzer.analyze_sentiment()
    
    # Extract topics for different sentiments
    analyzer.extract_topics(n_topics=10, sentiment_filter='positive')
    analyzer.extract_topics(n_topics=10, sentiment_filter='negative')
    analyzer.extract_topics(n_topics=15, sentiment_filter=None)  # All reviews
    
    logger.info("NLP analysis completed!")

if __name__ == "__main__":
    main()