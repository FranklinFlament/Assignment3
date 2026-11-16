# Assignment3

# Research Paper Pipeline — Airflow

Automated PDF processing pipeline using Apache Airflow, PyMuPDF4LLM, Ollama, SQLite, HTML and Discord.

## 1. Requirements

- Python 3.11
- [uv](https://docs.astral.sh/uv/)
- [Ollama](https://ollama.com/download/mac)
- macOS (tested on Apple Silicon)

## 2. Install Python dependencies

Clone the repository and open its directory:

```bash
git clone https://github.com/FranklinFlament/Assignment3
cd Assignment3
```

Create the virtual environment and install dependencies:

```bash
uv venv --python 3.11 .venv
source .venv/bin/activate
uv pip install -r requirements.txt
```

## 3. Set up Ollama

Install Ollama from https://ollama.com/download/mac and open the application.

Download the model:

```bash
ollama pull llama3.2:3b
```

Check that Ollama is running:

```bash
curl http://localhost:11434/v1/models
```

## 4. Start Airflow

Open **Terminal 1** in the project directory:

```bash
source .venv/bin/activate

export AIRFLOW_HOME="$(pwd)/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export DISCORD_WEBHOOK_URL="YOUR_DISCORD_WEBHOOK_URL"

airflow standalone
```

Keep this terminal running.

Open **Terminal 2** in the same project directory:

```bash
source .venv/bin/activate

export AIRFLOW_HOME="$(pwd)/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False

airflow dags list
```

## 5. Open Airflow

Open http://localhost:8080.

Log in with:

- Username: `admin`
- Password: found in `airflow/simple_auth_manager_passwords.json.generated`

Find the DAG named `paper_pipeline`.

## 6. Run the pipeline

Create the input directory if needed:

```bash
mkdir -p ResearchLibrary/papers
```

Add PDF files to:

```text
ResearchLibrary/papers/
```

In the Airflow web interface:

1. Open `paper_pipeline`.
2. Click **Trigger** to run it manually.
3. Wait for the tasks to complete.

The DAG also runs automatically every hour.

## 7. View the results

Open the generated HTML catalog:

```bash
open ResearchLibrary/output/index.html
```

The catalog supports text search and category filtering.

Check the SQLite database:

```bash
sqlite3 data/papers.db "SELECT filename, title, category FROM papers;"
```

Newly processed papers also trigger a Discord notification.

## 8. Pipeline tasks

```text
scan_papers
    |
process_papers
    |
extract_metadata
    |
save_metadata
    |----------------|
generate_html    notify_discord
```

Previously processed PDF filenames are skipped using SQLite. The database and generated Markdown files are stored locally in `data/`.

## Google Drive Synchronization

The pipeline monitors a Google Drive-synced folder for new PDF files.

On macOS, install [Google Drive for desktop](https://www.google.com/drive/download/) and sign in to your Google account.

Create the following folder in Google Drive:

```text
Mon Drive/
└── ResearchLibrary/
    └── papers/
```

Create a symbolic link from the project to the synchronized folder:

```bash
cd ~/Desktop/Assignment3

DRIVE_ACCOUNT="GoogleDrive-YOUR_EMAIL@gmail.com"
DRIVE="$HOME/Library/CloudStorage/$DRIVE_ACCOUNT/Mon Drive/ResearchLibrary/papers"

mkdir -p "$DRIVE"
ln -s "$DRIVE" ResearchLibrary/papers
```

**Note:** The `ln -s` command assumes that `ResearchLibrary/papers` does not already exist. If it exists, move or back up its contents first.

Any new PDF uploaded to this Google Drive folder becomes accessible locally and is automatically detected by the scheduled Airflow DAG.

The synchronization was successfully tested: a PDF uploaded through Google Drive was processed by the pipeline, and a Discord notification was received.

## Notes

- Ollama must be running before triggering the DAG.
- The Discord webhook URL must be configured before starting Airflow.
