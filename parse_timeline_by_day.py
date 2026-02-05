"""Parse Google Timeline semantic segments into day-by-day slices."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


def parse_iso8601(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def split_segment_by_day(start: datetime, end: datetime) -> Iterable[Tuple[datetime, datetime]]:
    if end < start:
        raise ValueError(f"Segment end is before start: {start.isoformat()} -> {end.isoformat()}")
    if end == start:
        yield start, end
        return

    current = start
    while current.date() < end.date():
        next_midnight = datetime.combine(
            current.date() + timedelta(days=1),
            time.min,
            tzinfo=current.tzinfo,
        )
        yield current, next_midnight
        current = next_midnight

    yield current, end


def build_segment_details(segment: Dict[str, Any], include_timeline_points: bool) -> Dict[str, Any]:
    if "activity" in segment:
        activity = segment.get("activity", {})
        top = activity.get("topCandidate", {})
        return {
            "segmentType": "activity",
            "activityType": top.get("type"),
            "activityProbability": top.get("probability"),
            "segmentProbability": activity.get("probability"),
            "distanceMeters": activity.get("distanceMeters"),
            "startPoint": activity.get("start", {}).get("latLng"),
            "endPoint": activity.get("end", {}).get("latLng"),
        }

    if "visit" in segment:
        visit = segment.get("visit", {})
        top = visit.get("topCandidate", {})
        return {
            "segmentType": "visit",
            "visitProbability": visit.get("probability"),
            "placeId": top.get("placeId"),
            "semanticType": top.get("semanticType"),
            "placeLocation": top.get("placeLocation", {}).get("latLng"),
        }

    path = segment.get("timelinePath", [])
    details: Dict[str, Any] = {
        "segmentType": "timelinePath",
        "pointCount": len(path),
    }
    if path:
        details["firstPoint"] = path[0].get("point")
        details["lastPoint"] = path[-1].get("point")
    if include_timeline_points:
        details["timelinePath"] = path
    return details


def parse_by_day(
    segments: List[Dict[str, Any]],
    include_timeline_points: bool,
) -> Dict[str, List[Dict[str, Any]]]:
    days: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for index, segment in enumerate(segments):
        start = parse_iso8601(segment["startTime"])
        end = parse_iso8601(segment["endTime"])
        details = build_segment_details(segment, include_timeline_points)

        for slice_start, slice_end in split_segment_by_day(start, end):
            duration_minutes = round((slice_end - slice_start).total_seconds() / 60.0, 2)
            day_key = slice_start.date().isoformat()
            days[day_key].append(
                {
                    "segmentIndex": index,
                    "startTime": slice_start.isoformat(),
                    "endTime": slice_end.isoformat(),
                    "durationMinutes": duration_minutes,
                    "originalStartTime": segment["startTime"],
                    "originalEndTime": segment["endTime"],
                    **details,
                }
            )

    for entries in days.values():
        entries.sort(key=lambda item: item["startTime"])

    return {day: days[day] for day in sorted(days)}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Parse timeline.json and group semantic segments by day."
    )
    parser.add_argument(
        "input",
        nargs="?",
        default="timeline.json",
        help="Path to input JSON file (default: timeline.json)",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="timeline_by_day.json",
        help="Path to output JSON file (default: timeline_by_day.json)",
    )
    parser.add_argument(
        "--include-timeline-points",
        action="store_true",
        help="Include full timelinePath point arrays in output.",
    )
    parser.add_argument(
        "--stdout",
        action="store_true",
        help="Print result to stdout instead of writing to a file.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    data = json.loads(input_path.read_text(encoding="utf-8"))
    segments = data.get("semanticSegments", [])
    if not isinstance(segments, list):
        raise ValueError("Invalid file format: 'semanticSegments' must be a list.")

    grouped = parse_by_day(
        segments=segments,
        include_timeline_points=args.include_timeline_points,
    )
    output = {
        "sourceFile": str(input_path),
        "dayCount": len(grouped),
        "segmentCount": len(segments),
        "days": grouped,
    }

    if args.stdout:
        print(json.dumps(output, indent=2, ensure_ascii=False))
        return 0

    output_path = Path(args.output)
    output_path.write_text(
        json.dumps(output, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"Wrote day-by-day output to: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
