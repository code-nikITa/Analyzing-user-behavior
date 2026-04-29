from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path


SOURCES = ["organic", "ads", "referral", "email"]
DEVICES = ["mobile", "desktop"]


def weighted_choice(rng: random.Random, values: list[str], weights: list[float]) -> str:
    return rng.choices(values, weights=weights, k=1)[0]


def make_timestamp(base_date: datetime, hour: int, minute: int) -> datetime:
    return base_date.replace(hour=hour, minute=minute, second=0, microsecond=0)


def generate_dataset(output_path: Path) -> None:
    rng = random.Random(42)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    start_date = datetime(2024, 1, 1)
    users_per_day = 18
    total_days = 10
    user_id = 1000
    rows: list[dict[str, str]] = []

    signup_prob = {"organic": 0.80, "ads": 0.58, "referral": 0.86, "email": 0.77}
    onboarding_prob = {"organic": 0.64, "ads": 0.45, "referral": 0.71, "email": 0.62}
    purchase_prob = {"organic": 0.51, "ads": 0.33, "referral": 0.58, "email": 0.47}

    for day_offset in range(total_days):
        cohort_date = start_date + timedelta(days=day_offset)

        for _ in range(users_per_day):
            user_id += 1
            source = weighted_choice(rng, SOURCES, [0.35, 0.30, 0.20, 0.15])
            device = weighted_choice(rng, DEVICES, [0.62, 0.38])

            visit_time = make_timestamp(
                cohort_date,
                hour=rng.randint(8, 22),
                minute=rng.randint(0, 59),
            )
            rows.append(
                {
                    "user_id": str(user_id),
                    "event_time": visit_time.isoformat(sep=" "),
                    "event_name": "visit",
                    "source": source,
                    "device": device,
                }
            )

            signed_up = rng.random() < signup_prob[source]
            onboarded = False
            purchased = False

            if signed_up:
                signup_time = visit_time + timedelta(minutes=rng.randint(5, 180))
                rows.append(
                    {
                        "user_id": str(user_id),
                        "event_time": signup_time.isoformat(sep=" "),
                        "event_name": "signup",
                        "source": source,
                        "device": device,
                    }
                )

                onboarded = rng.random() < onboarding_prob[source]
                if onboarded:
                    onboarding_time = signup_time + timedelta(hours=rng.randint(2, 48))
                    rows.append(
                        {
                            "user_id": str(user_id),
                            "event_time": onboarding_time.isoformat(sep=" "),
                            "event_name": "onboarding_complete",
                            "source": source,
                            "device": device,
                        }
                    )

                    purchased = rng.random() < purchase_prob[source]
                    if purchased:
                        purchase_time = onboarding_time + timedelta(hours=rng.randint(6, 72))
                        rows.append(
                            {
                                "user_id": str(user_id),
                                "event_time": purchase_time.isoformat(sep=" "),
                                "event_name": "first_purchase",
                                "source": source,
                                "device": device,
                            }
                        )

                if purchased:
                    retention_profile = [0.0, 0.84, 0.73, 0.66, 0.57, 0.52, 0.46, 0.41]
                elif onboarded:
                    retention_profile = [0.0, 0.58, 0.46, 0.39, 0.31, 0.27, 0.24, 0.19]
                else:
                    retention_profile = [0.0, 0.37, 0.26, 0.19, 0.14, 0.11, 0.09, 0.06]

                for retention_day in range(1, 8):
                    if rng.random() < retention_profile[retention_day]:
                        session_date = (signup_time + timedelta(days=retention_day)).date()
                        session_time = datetime.combine(session_date, datetime.min.time()) + timedelta(
                            hours=rng.randint(9, 22),
                            minutes=rng.randint(0, 59),
                        )
                        rows.append(
                            {
                                "user_id": str(user_id),
                                "event_time": session_time.isoformat(sep=" "),
                                "event_name": "session",
                                "source": source,
                                "device": device,
                            }
                        )
            elif rng.random() < 0.12:
                return_time = visit_time + timedelta(days=1, hours=rng.randint(1, 8))
                rows.append(
                    {
                        "user_id": str(user_id),
                        "event_time": return_time.isoformat(sep=" "),
                        "event_name": "session",
                        "source": source,
                        "device": device,
                    }
                )

    rows.sort(key=lambda row: (int(row["user_id"]), row["event_time"], row["event_name"]))

    with output_path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(
            csvfile,
            fieldnames=["user_id", "event_time", "event_name", "source", "device"],
        )
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    project_root = Path(__file__).resolve().parents[1]
    generate_dataset(project_root / "data" / "user_events.csv")
