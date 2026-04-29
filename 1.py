from pathlib import Path

from src.analyze import run_analysis
from src.generate_data import generate_dataset


if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent
    generate_dataset(project_root / "data" / "user_events.csv")
    run_analysis(project_root)
