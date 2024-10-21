# 1 FUNZIONE PIPELINE

from airflow.example_dags.example_external_task_marker_dag import start_date


def extract_and_filter_data(start_date=None, end_date=None):
    """
    Estrae i dati da Google Sheets e li filtra in base all'intervallo di date specificato.

    Args:
        start_date: Data di inizio dell'intervallo (formato 'YYYY-MM-DD').
        end_date: Data di fine dell'intervallo (formato 'YYYY-MM-DD').
            Se end_date è None, verrà utilizzato start_date per un singolo giorno.

    Returns:
        df_ga_filtered: DataFrame dei dati di Google Analytics filtrati.
        df_budget: DataFrame dei dati di budget.
    """
    import gspread
    from google.oauth2.service_account import Credentials
    import pandas as pd
    import time

    # Configurazione delle credenziali
    creds_path = '/Users/Alberto/PycharmProjects/Pipeline/dags/innovation-engineer-2.json'
    scopes = ['https://www.googleapis.com/auth/spreadsheets']

    try:
        creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
        client = gspread.authorize(creds)
        print("Autenticazione Google Sheets riuscita.")
    except Exception as e:
        print(f"Errore durante la configurazione delle credenziali: {e}")
        return

    # Estrazione dei dati dai Google Sheets con meccanismo di retry
    max_retries = 3
    for attempt in range(max_retries):
        try:
            sheet_id_budget = '1412wCpCV0TQKaPHuWBM5_Ujca4f9_H8lqCJRyfoWcE0'
            sheet_budget = client.open_by_key(sheet_id_budget)
            worksheet_budget = sheet_budget.worksheet('Forecasts')

            sheet_id_ga = '18VtAi1StMUfblg4NahC5eyoFw8oCWYmLckk8F_kZ6vY'
            sheet_ga = client.open_by_key(sheet_id_ga)
            worksheet_ga = sheet_ga.worksheet('GA')

            # Conversione dei dati in DataFrame
            data_budget = worksheet_budget.get_all_records()
            df_budget = pd.DataFrame(data_budget)

            data_ga = worksheet_ga.get_all_records()
            df_ga = pd.DataFrame(data_ga)

            print("Dati estratti correttamente da Google Sheets.")
            break
        except gspread.exceptions.APIError as e:
            print(
                f"Errore API durante l'estrazione dei dati (tentativo {attempt + 1} di {max_retries}): {e}. Riprovo...")
            time.sleep(2)  # Attesa prima di un nuovo tentativo
        except Exception as e:
            print(f"Errore generico durante l'estrazione dei dati (tentativo {attempt + 1} di {max_retries}): {e}")
            time.sleep(2)
        else:
            print(
                f"Errore persistente dopo {max_retries} tentativi. Verifica manualmente lo stato dell'API o della connessione.")
            return

    # Conversione della colonna di data in formato datetime con gestione degli errori
    try:
        df_ga['Date'] = pd.to_datetime(df_ga['event_date'], format='%Y%m%d')
        print("Conversione delle date completata.")
    except KeyError:
        print(
            'La colonna "event_date" non è presente nel dataframe df_ga. Potrebbe esserci stata una modifica nella struttura dei dati.')
        return
    except Exception as e:
        print(f"Errore durante la conversione delle date: {e}")
        return

    # Se end_date non è specificata, si considera la stessa data di start_date
    if start_date:
        try:
            start_date = pd.to_datetime(start_date)
            end_date = pd.to_datetime(end_date) if end_date else start_date

            # Trova le date minime e massime nel dataset
            min_date = df_ga['Date'].min()
            max_date = df_ga['Date'].max()

            # Verifica la validità delle date
            if start_date < min_date or start_date > max_date:
                print(f"Hai inserito una data di inizio ({start_date.date()}) non presente nel dataset. Riprova.")
                return

            if end_date < min_date:
                print(f"Hai inserito una data di fine ({end_date.date()}) antecedente al range disponibile. Riprova.")
                return

            # Se l'end_date supera il range disponibile, lo adatta alla data massima presente
            if end_date > max_date:
                print(
                    f"La data di fine ({end_date.date()}) supera l'ultima data disponibile ({max_date.date()}). Verrà utilizzata la data {max_date.date()} come ultima data di riferimento.")
                end_date = max_date

            # Filtraggio dei dati per l'intervallo specificato
            df_ga_filtered = df_ga[(df_ga['Date'] >= start_date) & (df_ga['Date'] <= end_date)]

            # Verifica se i DataFrame sono vuoti
            if df_ga_filtered.empty:
                print("L'intervallo di date selezionato non ha restituito risultati. Riprova con un altro intervallo.")
                return
        except Exception as e:
            print(f"Errore durante il filtraggio delle date: {e}")
            return
    else:
        df_ga_filtered = df_ga

    base_path = '/Users/Alberto/PycharmProjects/Pipeline/dags/'
    # Salvataggio dei dati filtrati in file CSV
    try:
        df_ga_filtered.to_csv(f'{base_path}filtered_ga_data.csv', index=False)
        df_budget.to_csv(f'{base_path}data_budget.csv', index=False)
        print("Dati filtrati e salvati in CSV.")
    except Exception as e:
        print(f"Errore durante il salvataggio dei dati in CSV: {e}")
        return

    print("Estrazione e filtraggio dei dati completati.")

start_date = '2024-08-01'
end_date = '2024-08-09'
extract_and_filter_data(start_date= start_date, end_date= end_date)

# 2 FUNZIONE PIPELINE
import pandas as pd
import warnings
import re

# 2 FUNZIONE PIPELINE

def clean_data():
    """
    Carica i dataset estratti dalla pipeline, applica le operazioni di pulizia specificate,
    e restituisce i dataframe puliti.

    Returns:
        df_budget_cleaned: DataFrame del budget ripulito.
        df_ga_cleaned: DataFrame dei dati di Google Analytics filtrati e puliti.
    """
    # Disattivare i FutureWarning di Pandas
    warnings.filterwarnings('ignore', category=FutureWarning)

    base_path = '/Users/Alberto/PycharmProjects/Pipeline/dags/'


    # Caricamento dei dati
    df_budget = pd.read_csv(f'{base_path}data_budget.csv')
    df_ga = pd.read_csv(f'{base_path}filtered_ga_data.csv')

    # Pulizia df_budget
    # Riempire i NaN nella colonna 'region' con 'Unknown region'
    df_budget['Region'] = df_budget['Region'].fillna('Unknown region')

    # Riempire i NaN nella colonna 'forecasted_purchases' con la media per 'region' e 'country'
    df_budget['Forecasted Purchases'] = df_budget.groupby(['Region', 'Country'])['Forecasted Purchases'].transform(
        lambda x: x.fillna(x.mean())
    )

    # Salvataggio del dataset pulito df_budget
    df_budget.to_csv(f'{base_path}cleaned_data_budget.csv', index=False)
    print("Pulizia di df_budget completata e salvata in 'cleaned_data_budget.csv'.")

    # Pulizia df_ga
    # Filtrare gli eventi rilevanti per l'analisi
    event_names_to_keep = [
        'session_start', 'view_item', 'purchase', 'scroll', 'click', 'external_clickout', 'click_view_item_list'
    ]
    event_param_keys_to_keep = ['page_location', 'page_referrer']

    # Filtrare le righe con event_name tra quelli di interesse
    df_ga_filtered = df_ga[df_ga['event_name'].isin(event_names_to_keep)]

    # Mantenere solo le righe dove 'event_param_key' è tra quelli di interesse
    df_ga_filtered = df_ga_filtered[df_ga_filtered['event_param_key'].isin(event_param_keys_to_keep)]

    # Rimuovere le colonne non necessarie
    columns_to_drop = ['event_param_float_value', 'user_id', 'event_timestamp']
    df_ga_filtered.drop(columns=columns_to_drop, inplace=True, errors='ignore')

    # Parsing degli URL per mantenere solo il dominio principale
    def parse_url(url):
        match = re.match(r'(https?://(?:www\.)?[^/]+)', url)
        return match.group(1) if match else url

    # Applica il parsing alla colonna 'event_param_string_value' per estrarre i domini
    df_ga_filtered['event_param_string_value'] = df_ga_filtered['event_param_string_value'].apply(parse_url)

    # Salvataggio del dataset pulito df_ga_filtered
    df_ga_filtered.to_csv(f'{base_path}cleaned_filtered_ga_data.csv', index=False)
    print("Pulizia di df_ga completata e salvata in 'cleaned_filtered_ga_data.csv'.")


# Eseguire la funzione di pulizia
clean_data()


# 3 FUNZIONE PIPELINE
def upload_to_gcs_and_bigquery(bucket_name, project_id, dataset_id):
    """
    Carica i file CSV puliti su Google Cloud Storage e li importa in BigQuery.

    Args:
        bucket_name: Il nome del bucket GCS.
        project_id: L'ID del progetto Google Cloud.
        dataset_id: L'ID del dataset BigQuery.
    """
    from google.cloud import storage
    from google.cloud import bigquery
    import os

    base_path = '/Users/Alberto/PycharmProjects/Pipeline/dags/'

    # Imposta le credenziali di autenticazione
    creds_path = '/Users/Alberto/PycharmProjects/Pipeline/dags/innovation-engineer-2.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path

    # Definisci i nomi dei file CSV locali
    local_path_budget = f'{base_path}cleaned_data_budget.csv'
    local_path_ga = f'{base_path}cleaned_filtered_ga_data.csv'
    uri_budget = f'gs://{bucket_name}/cleaned_data_budget.csv'
    uri_ga = f'gs://{bucket_name}/cleaned_filtered_ga_data.csv'

    # Carica i file su Google Cloud Storage
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        # Caricamento del file budget
        blob_budget = bucket.blob('cleaned_data_budget.csv')
        blob_budget.upload_from_filename(local_path_budget)
        print(f'File budget caricato su GCS: {uri_budget}')

        # Caricamento del file GA
        blob_ga = bucket.blob('cleaned_filtered_ga_data.csv')
        blob_ga.upload_from_filename(local_path_ga)
        print(f'File GA caricato su GCS: {uri_ga}')

    except Exception as e:
        print(f'Errore durante il caricamento su GCS: {e}')
        return

    # Carica i dati in BigQuery
    try:
        bigquery_client = bigquery.Client()

        # Imposta le configurazioni di caricamento per il budget
        table_id_budget = f"{project_id}.{dataset_id}.budget_data"
        job_config_budget = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # Salta l'intestazione se necessario
            autodetect=True,
        )
        load_job_budget = bigquery_client.load_table_from_uri(
            uri_budget,
            table_id_budget,
            job_config=job_config_budget,
        )
        load_job_budget.result()  # Attendi il completamento del job
        print(f'Dati del budget caricati in BigQuery nella tabella: {table_id_budget}')

        # Imposta le configurazioni di caricamento per GA
        table_id_ga = f"{project_id}.{dataset_id}.ga_data"
        job_config_ga = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # Salta l'intestazione se necessario
            autodetect=True,
        )
        load_job_ga = bigquery_client.load_table_from_uri(
            uri_ga,
            table_id_ga,
            job_config=job_config_ga,
        )
        load_job_ga.result()  # Attendi il completamento del job
        print(f'Dati di Google Analytics caricati in BigQuery nella tabella: {table_id_ga}')

    except Exception as e:
        print(f'Errore durante il caricamento su BigQuery: {e}')


bucket_name = 'innovationengineer'
project_id = 'innovation-engineer'
dataset_id = 'marketing_data'

upload_to_gcs_and_bigquery(bucket_name=bucket_name, project_id=project_id, dataset_id=dataset_id)
