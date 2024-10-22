# **Data Pipeline with Google Sheets and Google Analytics Integration**

## **Descrizione**

Questo progetto ha come obiettivo l'implementazione di una pipeline resiliente che riceva dati da Google Sheet e da Google Analytics, automatizzi l'estrazione giornaliera e svolga analisi sui dati per fornire insight significativi.
La pipeline sviluppata estrae dati da Google Sheets (inclusi quelli relativi a budget e Google Analytics), li pulisce e li carica su Google Cloud, rendendoli disponibili per l'analisi tramite BigQuery. L'intero processo è stato automatizzato utilizzando Apache Airflow.
L'obiettivo principale quindi è trasformare, pulire e caricare questi dati su Google Cloud Storage (GCS) e successivamente in BigQuery per analisi avanzate.

## **Funzionalità della Pipeline**

- **Estrazione dei Dati**:
  - Estrazione automatica dei dati da Google Sheets utilizzando le API di Google Sheets.
  - I dati riguardano informazioni di marketing e budget estratti dai fogli di calcolo.

- **Pulizia dei dataset**:
  - Utilizzo di Pandas per trasformare i dati estratti, riempiendo valori mancanti, ripulendo i dati per una corretta analisi.
  - Creazione di copie pulite dei dataset che vengono salvate come file CSV.

- **Caricamento su GCS e BigQuery**:
  - Salvataggio dei file CSV ripuliti su Google Cloud Storage per garantire una copia persistente dei dati.
  - Caricamento automatico dei file salvati su BigQuery per facilitare l'esecuzione di query SQL e analisi avanzate.

- **Automazione con Airflow**:
  - Gestione delle varie fasi della pipeline (estrazione, pulizia, caricamento) tramite DAG di Apache Airflow.
  - Scheduler configurato per eseguire la pipeline su base giornaliera e per gestire anche intervalli di date specifiche.

## **Struttura del Codice**

Il codice è strutturato in tre funzioni principali:

- **`extract_and_filter_data`**: Estrae i dati da Google Sheets e li filtra per un intervallo di date specificato. In alternativa, la versione estesa permette l'estrazione diretta dei dati da Google Analytics per analizzare metriche di sessione e comportamento degli utenti.

- **`clean_data`**: Pulisce i dati estratti, gestendo valori mancanti e rimuovendo colonne superflue. I dati vengono salvati in formato CSV in una cartella temporanea prima di essere caricati su GCS.

- **`upload_to_gcs_and_bigquery`**: Carica i file CSV puliti su Google Cloud Storage e successivamente li importa in tabelle BigQuery per permettere l'analisi dei dati tramite SQL.

