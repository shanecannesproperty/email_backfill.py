#!/usr/bin/env python3
from __future__ import annotations

import argparse
import calendar
from datetime import datetime, timezone


DATE_FORMAT = "%Y/%m/%d"


def parse_date(value: str):
    return datetime.strptime(value, DATE_FORMAT).date()


def format_date(value):
    return value.strftime(DATE_FORMAT)


def add_months(value, months: int):
    month_index = (value.year * 12 + (value.month - 1)) + months
    year = month_index // 12
    month = (month_index % 12) + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return value.replace(year=year, month=month, day=day)


def first_day_of_next_month(value):
    if value.month == 12:
        return value.replace(year=value.year + 1, month=1, day=1)
    return value.replace(month=value.month + 1, day=1)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--months-back", type=int, default=12)
    return parser.parse_args()


def main():
    args = parse_args()

    if bool(args.start_date) ^ bool(args.end_date):
        raise SystemExit("Both --start-date and --end-date must be provided together.")

    ranges = []
    if args.start_date and args.end_date:
        start_date = parse_date(args.start_date)
        end_date = parse_date(args.end_date)
        if start_date >= end_date:
            raise SystemExit("--start-date must be earlier than --end-date.")
        ranges.append(("manual", start_date, end_date))
    else:
        if args.months_back <= 0:
            raise SystemExit("--months-back must be a positive integer.")

        end_date = datetime.now(timezone.utc).date()
        start_date = add_months(end_date, -args.months_back)

        current = start_date
        while current < end_date:
            chunk_end = min(first_day_of_next_month(current), end_date)
            ranges.append((f"auto-{current.strftime('%Y-%m')}", current, chunk_end))
            current = chunk_end

    for label, start_date, end_date in ranges:
        print(f"{label}|{format_date(start_date)}|{format_date(end_date)}")


if __name__ == "__main__":
    main()
