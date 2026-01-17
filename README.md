# BigDataFraude\_ML-GraphX

Détection de fraude bancaire avec Spark MLlib et GraphX

## Technologies

* Apache Spark
* PySpark
* MLlib
* GraphX / GraphFrames
* Spark Streaming
* Grafana

## Dataset

Credit Card Fraud Detection (Kaggle):

https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud

## Pipeline

CSV → Nettoyage → SQL Analytics → ML → Graph → Streaming → Dashboard

# Guide d'Installation et d'Exécution du Projet

## 1\. Clonage et Lancement de l'Infrastructure 

**# Créer le dossier de travail et cloner le projet**

mkdir BIGDATA

cd BIGDATA

git clone https://github.com/AbbessAhlem/BigDataFraude\_ML-GraphX

cd BigDataFraude\_ML-GraphX



**# Lancer tous les services (Kafka, Spark, HDFS, InfluxDB, Grafana)**

docker compose up -d 



**# Vérifier que tous les conteneurs tournent correctement**

docker ps

2. Chargement des Données vers HDFS
---

**Pour que Spark puisse traiter les fichiers, nous devons déplacer le CSV du dossier local vers le système de fichiers distribué (HDFS).**



Bash



**# Étape 1 : Copier le dataset local vers le conteneur namenode**

docker cp data/creditcard.csv namenode:/tmp/creditcard.csv



**# Étape 2 : Entrer dans le conteneur namenode pour manipuler HDFS**

docker exec -it namenode bash



**# --- À l'intérieur du conteneur ---**

**# Créer le répertoire de destination dans HDFS**

hdfs dfs -mkdir -p /data/

**# Charger le fichier depuis /tmp vers HDFS**

hdfs dfs -put /tmp/creditcard.csv /data/

**# Vérifier la présence du fichier et donner les droits**

hdfs dfs -ls /data

hdfs dfs -chmod -R 777 /data

exit

**# --- Fin du conteneur ---**

## 3\. Accès à l'interface Jupyter

## Bash



**# Récupérer l'URL et le Token de connexion**

docker logs jupyter --tail 100

**# Action : Ctrl + Click sur le lien commençant par http://127.0.0.1:8888...**

4\. Exécution du Pipeline de Streaming

**Exécutez ces commandes soit dans le terminal de Jupyter, soit via docker exec.**



### A. Installation des dépendances Python :



Bash



pip install kafka-python influxdb-client

### B. Lancer le Producteur Kafka :



Bash



**# Ce script envoie les transactions vers le topic Kafka**

python3 streaming/kafka\_producer.py

### C. Lancer le Traitement Spark Streaming :



Bash



**# Ce script consomme les flux Kafka et enregistre les résultats dans InfluxDB**

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10\_2.12:3.5.0 \\

/home/jovyan/streaming/spark\_streaming.py



# Liens Utiles (URLs) :

Jupyter Notebook : http://localhost:8888



Grafana (Dashboards) : http://localhost:3000 (Login: admin / admin)



Spark Master UI : http://localhost:8080



HDFS Namenode UI : http://localhost:9870

