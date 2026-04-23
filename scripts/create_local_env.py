from pathlib import Path
import shutil


def main() -> int:
    project_root = Path(__file__).resolve().parent.parent
    example_path = project_root / '.env.example'
    env_path = project_root / '.env'

    if env_path.exists():
        print(f'.env already exists at {env_path}')
        return 0
    if not example_path.exists():
        print(f'.env.example was not found at {example_path}')
        return 1

    shutil.copyfile(example_path, env_path)
    print(f'Created {env_path} from .env.example')
    print('Now open .env and paste your real OPENAI_API_KEY before running the app.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
