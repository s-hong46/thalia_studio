# ComedyCoach

ComedyCoach is a Flask based rehearsal workspace for stand up comedy writing and performance practice.
It supports draft editing, recording, transcript analysis, focused delivery feedback, and optional reference video retrieval.

This public snapshot has been cleaned for GitHub publication. Local credentials, generated media, caches, test databases, and large runtime artifacts are intentionally excluded.

## What is included

- A browser based writing and rehearsal workflow
- Transcript alignment with matched, improvised, and missed spans
- Focused delivery notes with practice steps
- Optional ASR, TTS, and reference retrieval integrations through environment variables
- Dataset ingestion utilities and backend services
- A pytest suite for core backend behavior

## What is not included

- Real API keys, tokens, or local database credentials
- Large stand up video datasets and generated preview clips
- Recorded rehearsal media and generated TTS outputs
- Local caches, logs, and scratch scripts used during development

## Project structure

```text
app/
  routes/              Flask routes and API handlers
  services/            Analysis, retrieval, ASR, TTS, and dataset services
  static/              Frontend JS, CSS, and generated media folders
  templates/           HTML templates
scripts/               Utility scripts for setup and dataset indexing
tests/                 Automated tests
run.py                 Local development entrypoint
```

## Requirements

- Python 3.11+
- Optional: ffmpeg / ffprobe for audio and preview generation
- Optional: OpenAI credentials for model backed analysis, ASR, and TTS
- Optional: Pinecone credentials for vector retrieval
- Optional: MySQL if you do not want to use SQLite locally

## Quick start

1. Create and activate a virtual environment.
2. Install dependencies.

   ```bash
   pip install -r requirements.txt
   ```

3. Create a local environment file.

   ```bash
   python scripts/create_local_env.py
   ```

4. Edit `.env` and set the variables you want to use.
   The example defaults to SQLite so the app can boot without a MySQL server.

5. Start the app.

   ```bash
   python run.py
   ```

6. Open `http://127.0.0.1:5000`.

## Configuration

The main environment variables are:

- `OPENAI_API_KEY`
- `PINECONE_API_KEY`
- `DATABASE_URL`
- `MYSQL_URL` as a legacy fallback
- `VIDEO_DATASET_ROOT`
- `AUTO_VIDEO_DATASET_INGEST`
- `DISABLE_VIDEO_DATASET_INGEST`
- `DISABLE_REFERENCE_LLM_ENRICHMENT`

Example local database setting:

```env
DATABASE_URL=sqlite:///artifacts/dev.db
```

## Running tests

```bash
python -m pytest -q
```

A simple local setup is:

```bash
export DATABASE_URL='sqlite:///:memory:'
export DISABLE_VIDEO_DATASET_INGEST='1'
python -m pytest -q
```

On PowerShell:

```powershell
$env:DATABASE_URL='sqlite:///:memory:'
$env:DISABLE_VIDEO_DATASET_INGEST='1'
python -m pytest -q
```

## Notes for publishing

Before pushing to a public GitHub repository, rotate any credential that may have existed in prior local copies.
If you need a cleaner open source boundary, keep private datasets and experimental notebooks in a separate non public repository.

## License

This repository is released under the MIT License.
