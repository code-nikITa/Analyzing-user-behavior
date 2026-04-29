from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd


STAGE_LABELS = {
    "visit": "Визит",
    "signup": "Регистрация",
    "onboarding_complete": "Онбординг",
    "first_purchase": "Первая покупка",
}


def load_sql(project_root: Path, relative_path: str) -> str:
    return (project_root / relative_path).read_text(encoding="utf-8")


def save_summary(project_root: Path, summary_lines: list[str]) -> None:
    summary_path = project_root / "outputs" / "summary.md"
    summary_path.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")


def run_analysis(project_root: Path) -> None:
    data_path = project_root / "data" / "user_events.csv"
    output_dir = project_root / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)

    events = pd.read_csv(data_path, parse_dates=["event_time"])

    connection = sqlite3.connect(":memory:")
    try:
        events.to_sql("user_events", connection, index=False, if_exists="replace")

        funnel = pd.read_sql_query(load_sql(project_root, "sql/funnel.sql"), connection)
        retention = pd.read_sql_query(load_sql(project_root, "sql/retention.sql"), connection)
    finally:
        connection.close()

    funnel["stage_label"] = funnel["event_name"].map(STAGE_LABELS)
    funnel["conversion_from_previous_pct"] = (
        funnel["users"].div(funnel["users"].shift(1)).mul(100).round(2)
    )
    funnel["conversion_from_first_pct"] = (
        funnel["users"].div(funnel.loc[0, "users"]).mul(100).round(2)
    )
    funnel.loc[0, "conversion_from_previous_pct"] = 100.0
    funnel["loss_users"] = funnel["users"].shift(1) - funnel["users"]
    funnel["loss_rate_pct"] = (
        funnel["loss_users"].div(funnel["users"].shift(1)).mul(100).round(2)
    )

    retention["cohort_date"] = pd.to_datetime(retention["cohort_date"]).dt.date.astype(str)
    overall_retention = (
        retention.groupby("day_number", as_index=False)
        .agg(retained_users=("retained_users", "sum"), cohort_size=("cohort_size", "sum"))
        .assign(
            retention_rate=lambda df: (
                df["retained_users"].div(df["cohort_size"]).mul(100).round(2)
            )
        )
    )
    retention_pivot = retention.pivot(
        index="cohort_date", columns="day_number", values="retention_rate"
    ).round(2)

    bottleneck_row = funnel.iloc[1:]["loss_rate_pct"].idxmax()
    bottleneck = funnel.loc[bottleneck_row]
    previous_stage = funnel.loc[bottleneck_row - 1, "stage_label"]

    purchase_conversion = funnel.loc[funnel["event_name"] == "first_purchase", "conversion_from_first_pct"].iloc[0]
    day_1_retention = overall_retention.loc[overall_retention["day_number"] == 1, "retention_rate"].iloc[0]
    day_7_retention = overall_retention.loc[overall_retention["day_number"] == 7, "retention_rate"].iloc[0]

    funnel.drop(columns=["stage_label"]).to_csv(output_dir / "funnel.csv", index=False)
    retention.to_csv(output_dir / "retention_by_cohort.csv", index=False)
    overall_retention.to_csv(output_dir / "retention_overall.csv", index=False)
    retention_pivot.to_csv(output_dir / "retention_pivot.csv")

    summary_lines = [
        "# Результаты",
        "",
        f"- До первой покупки доходит {purchase_conversion:.2f}% пользователей от всех визитов.",
        (
            f"- Главное узкое место: этап `{previous_stage} -> {bottleneck['stage_label']}`. "
            f"На нём теряется {int(bottleneck['loss_users'])} пользователей "
            f"({bottleneck['loss_rate_pct']:.2f}%)."
        ),
        (
            f"- Retention снижается с {day_1_retention:.2f}% в Day 1 "
            f"до {day_7_retention:.2f}% в Day 7."
        ),
        (
            "- Рекомендация: сократить трение перед первой покупкой, добавить "
            "прогревочный сценарий после онбординга и тестировать welcome-offer "
            "для пользователей, которые завершили настройку, но не конвертировались."
        ),
    ]
    save_summary(project_root, summary_lines)

    print("\n".join(summary_lines))


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    run_analysis(root)
