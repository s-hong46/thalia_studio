# Contributing

Thanks for contributing to ComedyCoach.

## Development setup

1. Create a virtual environment.
2. Install dependencies with `pip install -r requirements.txt`.
3. Copy `.env.example` to `.env` with `python scripts/create_local_env.py`.
4. Prefer `DATABASE_URL=sqlite:///artifacts/dev.db` for local work unless you specifically need MySQL.

## Before opening a pull request

- Run `python -m pytest -q`
- Keep secrets out of committed files
- Do not commit generated assets, logs, or local datasets
- Add or update tests when changing backend behavior

## Code style

- Keep changes focused and easy to review
- Prefer small helpers over deeply nested logic
- Preserve graceful degradation when optional services are unavailable

## Security

If you discover a security issue, do not open a public issue with the secret or exploit details.
Follow the guidance in `SECURITY.md` instead.
