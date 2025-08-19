# clean-dw-project 
 
Ce projet met en place un pipeline complet de traitement de données issues d'avis clients en ligne, avec intégration de scraping, nettoyage, NLP, et chargement dans une base PostgreSQL via Airflow. 
 
## Structure du projet 
- google_maps_scraper.py : Scraping d'avis sur Google Maps 
- scraper_utils.py : Fonctions utilitaires pour le scraping 
- data_cleaning_pipeline.py : Nettoyage des donn‚es textuelles 
- nlp_analysis.py : Analyse NLP (tokenisation, sentiment, etc.) 
- load_to_postgres.py : Chargement des donn‚es nettoy‚es dans PostgreSQL 
- requirements.txt : D‚pendances Python 
- airflow-docker/ : Conteneurisation du pipeline avec Apache Airflow 
- data/, logs/ : R‚pertoires pour les fichiers bruts et les journaux 
 
## Technologies utilisées 
- Python 
- BeautifulSoup, Requests 
- pandas, nltk, spacy 
- PostgreSQL 
- Apache Airflow 
- Docker 
 
## Auteurs 
Houda Mouradi 

Marwa Takatri 

