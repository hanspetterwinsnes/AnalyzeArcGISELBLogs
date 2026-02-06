#!/usr/bin/env python3
import argparse
import os
import sys
import gzip
import json
import shlex
import dotenv
import re
from datetime import datetime, timezone
from pathlib import Path
from collections import Counter

try:
    import pandas as pd
except Exception:
    pd = None

dotenv.load_dotenv()  # Load .env if exists, for AWS credentials or other config

try:
    import boto3
    from botocore.exceptions import ClientError
except Exception:
    boto3 = None


def ensure_boto():
    if boto3 is None:
        print("boto3 is required. Install from requirements.txt and try again.")
        sys.exit(1)


def get_s3_client(profile=None, region=None):
    ensure_boto()
    if profile:
        session = boto3.Session(profile_name=profile, region_name=region)
    else:
        session = boto3.Session(region_name=region)
    return session.client("s3")


def list_objects(s3, bucket, prefix=""):
    paginator = s3.get_paginator("list_objects_v2")
    kwargs = {"Bucket": bucket, "Prefix": prefix}
    for page in paginator.paginate(**kwargs):
        for obj in page.get("Contents", []):
            yield obj


def download_object(s3, bucket, key, target_path):
    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    try:
        s3.download_file(bucket, key, target_path)
        return True
    except ClientError as e:
        print(f"Failed to download {key}: {e}")
        return False


def open_maybe_gz(path):
    if path.endswith(".gz"):
        return gzip.open(path, "rt", errors="replace")
    return open(path, "r", errors="replace")


def parse_elb_line(line):
    if not line or line.startswith("#"):
        return None
    parts = shlex.split(line)
    if len(parts) >= 12:
        timestamp = parts[1]
        client = parts[2] if len(parts) > 2 else ""
        try:
            req_proc = float(parts[4])
        except Exception:
            req_proc = None
        try:
            backend_proc = float(parts[5])
        except Exception:
            backend_proc = None
        try:
            resp_proc = float(parts[6])
        except Exception:
            resp_proc = None
        elb_status = parts[7]
        backend_status = parts[8]
        try:
            received = int(parts[9])
        except Exception:
            received = 0
        try:
            sent = int(parts[10])
        except Exception:
            sent = 0
        request = parts[12] if len(parts) > 12 else ""
        pattern = r'https?://[^?\s]+'
        # Search for the pattern
        match = re.search(pattern, request)
        if match:
            request = match.group(0)
            pattern = r'/rest/services/([^/]+/[^/?\s]+)'
            match = re.search(pattern, request)
            if match:
                mapservice = match.group(1)
            else:
                mapservice = ""
        return {
            "timestamp": timestamp,
            "client": client,
            "request_processing_time": req_proc,
            "backend_processing_time": backend_proc,
            "response_processing_time": resp_proc,
            "elb_status": elb_status,
            "backend_status": backend_status,
            "received_bytes": received,
            "sent_bytes": sent,
            "request": request,
            "mapservice": mapservice,
        }
    # parts = line.split()
    # client = next((p for p in parts if ":" in p and p.split(":")[0].count(".") == 3), "")
    # status = next((p for p in parts if p.isdigit() and 100 <= int(p) <= 599), None)
    # nums = [int(p) for p in parts if p.isdigit()]
    # received = nums[-2] if len(nums) >= 2 else 0
    # sent = nums[-1] if len(nums) >= 1 else 0
    # return {
    #     "timestamp": None,
    #     "client": client,
    #     "request_processing_time": None,
    #     "backend_processing_time": None,
    #     "response_processing_time": None,
    #     "elb_status": status,
    #     "backend_status": None,
    #     "received_bytes": received,
    #     "sent_bytes": sent,
    #     "request": "",
    # }


def analyze_files(paths):
    total = 0
    total_received = 0
    total_sent = 0
    status_counter = Counter()
    client_counter = Counter()
    url_counter = Counter()
    total_req_time = 0.0
    req_time_count = 0
    all_lines = []

    for p in paths:
        with open_maybe_gz(p) as fh:
            for line in fh:
                parsed = parse_elb_line(line.strip())
                if not parsed:
                    continue
                all_lines.append(parsed)
                total += 1
                total_received += parsed.get("received_bytes", 0) or 0
                total_sent += parsed.get("sent_bytes", 0) or 0
                status = parsed.get("elb_status") or parsed.get("backend_status")
                if status is not None:
                    try:
                        sc = int(status)
                        bucket = f"{sc//100}xx"
                        status_counter[bucket] += 1
                    except Exception:
                        status_counter[str(status)] += 1
                client = parsed.get("client", "")
                if client:
                    client_ip = client.split(":")[0]
                    client_counter[client_ip] += 1
                req = parsed.get("request", "")
                if req:
                    parts = req.split()
                    if len(parts) >= 2:
                        url = parts[1]
                        url_counter[url] += 1
                times = [parsed.get(k) for k in ("request_processing_time", "backend_processing_time", "response_processing_time")]
                times = [t for t in times if isinstance(t, (int, float))]
                if times:
                    total_req_time += sum(times)
                    req_time_count += 1

    result = {
        "total_requests": total,
        "total_received_bytes": total_received,
        "total_sent_bytes": total_sent,
        "status_counts": dict(status_counter),
        "top_clients": client_counter.most_common(10),
        "top_urls": url_counter.most_common(10),
        "avg_processing_time": (total_req_time / req_time_count) if req_time_count else None,
        "all_lines": all_lines,
    }
    return result


def find_local_log_files(directory):
    p = Path(directory)
    if not p.exists():
        return []
    files = [str(p / f) for f in sorted(os.listdir(directory)) if f.endswith(".log") or f.endswith(".gz") or f.endswith(".txt")]
    return files


def iso_to_dt(s):
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)


def write_excel(analysis_result, output_path):
    """Write analysis results to Excel file with multiple sheets."""
    if pd is None:
        print("pandas is required for Excel output. Install from requirements.txt and try again.")
        return
    
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        # Summary sheet
        summary_data = {
            "Metric": [
                "Total Requests",
                "Total Received Bytes",
                "Total Sent Bytes",
                "Average Processing Time (ms)",
            ],
            "Value": [
                analysis_result.get("total_requests", 0),
                analysis_result.get("total_received_bytes", 0),
                analysis_result.get("total_sent_bytes", 0),
                analysis_result.get("avg_processing_time", 0),
            ]
        }
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name="Summary", index=False)
        
        # All log lines sheet
        all_lines = analysis_result.get("all_lines", [])
        if all_lines:
            lines_df = pd.DataFrame(all_lines)
            lines_df.to_excel(writer, sheet_name="All Logs", index=False)
        
        # Status codes sheet
        status_counts = analysis_result.get("status_counts", {})
        status_df = pd.DataFrame(list(status_counts.items()), columns=["Status", "Count"])
        status_df = status_df.sort_values("Count", ascending=False)
        status_df.to_excel(writer, sheet_name="Status Codes", index=False)
        
        # Top clients sheet
        top_clients = analysis_result.get("top_clients", [])
        if top_clients:
            clients_df = pd.DataFrame(top_clients, columns=["Client IP", "Requests"])
            clients_df.to_excel(writer, sheet_name="Top Clients", index=False)
        
        # Top URLs sheet
        top_urls = analysis_result.get("top_urls", [])
        if top_urls:
            urls_df = pd.DataFrame(top_urls, columns=["URL", "Requests"])
            urls_df.to_excel(writer, sheet_name="Top URLs", index=False)
    
    print(f"Analysis written to {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Download ELB logs from S3 and analyze them")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--prefix", default="", help="S3 prefix for logs")
    parser.add_argument("--profile", default=None, help="AWS profile name")
    parser.add_argument("--region", default=None, help="AWS region")
    parser.add_argument("--local-dir", default="./elb_logs", help="Local directory to store logs")
    parser.add_argument("--start-date", help="ISO start date (inclusive), e.g. 2026-02-01")
    parser.add_argument("--end-date", help="ISO end date (inclusive)")
    parser.add_argument("--max-objects", type=int, default=0, help="Limit number of objects to download (0 = no limit)")
    parser.add_argument("--download-only", action="store_true")
    parser.add_argument("--analyze-only", action="store_true")
    parser.add_argument("--json-out", help="Write analysis to JSON file")
    parser.add_argument("--excel-out", help="Write analysis to Excel file")
    args = parser.parse_args()

    if args.analyze_only and not os.path.isdir(args.local_dir):
        print("Local dir not found for analyze-only mode.")
        sys.exit(1)

    if not args.analyze_only:
        s3 = get_s3_client(profile=args.profile, region=args.region)
        objs = list(list_objects(s3, args.bucket, args.prefix))
        if args.start_date:
            start = iso_to_dt(args.start_date)
        else:
            start = None
        if args.end_date:
            end = iso_to_dt(args.end_date)
        else:
            end = None

        to_download = []
        for o in objs:
            lm = o.get("LastModified")
            if start and lm.replace(tzinfo=timezone.utc) < start:
                continue
            if end and lm.replace(tzinfo=timezone.utc) > end:
                continue
            to_download.append(o.get("Key"))
            if args.max_objects and len(to_download) >= args.max_objects:
                break

        print(f"Found {len(to_download)} objects to download")
        os.makedirs(args.local_dir, exist_ok=True)
        downloaded = []
        for key in to_download:
            target = os.path.join(args.local_dir, os.path.basename(key))
            ok = download_object(s3, args.bucket, key, target)
            if ok:
                downloaded.append(target)

    else:
        downloaded = find_local_log_files(args.local_dir)

    if args.download_only:
        print(f"Downloaded {len(downloaded)} files into {args.local_dir}")
        return

    if not downloaded:
        print("No log files to analyze.")
        return

    summary = analyze_files(downloaded)
    print("Analysis summary:")
    print(json.dumps(summary, indent=2, default=str))
    if args.json_out:
        with open(args.json_out, "w") as fh:
            json.dump(summary, fh, indent=2, default=str)
    if args.excel_out:
        write_excel(summary, args.excel_out)


if __name__ == "__main__":
    # line = 'h2 2026-02-01T19:40:27.217624Z app/alb-new-gis-prod/51fa1287639c83c0 88.89.241.253:54514 10.2.64.202:6443 0.029 0.001 0.000 200 200 57 4186 "GET https://geodata.bymoslo.no:443/arcgis/rest/services/geodata/Parkering/MapServer/3?f=json HTTP/2.0" "Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.2 Mobile/15E148 Safari/604.1" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:eu-west-1:099702455984:targetgroup/tg-geodata-linux-rest/1fc05f6eee69c7ec "Root=1-697fac2b-37c1883c146f782b4c0531b4" "geodata.bymoslo.no" "arn:aws:acm:eu-west-1:099702455984:certificate/bb069ebc-f9d2-4737-8999-8b7f6772a012" 114 2026-02-01T19:40:27.187000Z "waf,forward" "-" "-" "10.2.64.202:6443" "200" "-" "-" TID_13c6539d4d103340847ff94500482a22 "-" "-" "-"'
    # r = parse_elb_line(line)
    # print(r)
    main()
