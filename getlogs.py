#!/usr/bin/env python3
import argparse
import os
import shutil
import sys
import gzip
import shlex
import dotenv
import re
import getservices
from datetime import datetime, timezone
from pathlib import Path
from collections import Counter
from datetime import datetime, timedelta

try:
    import pandas as pd
except Exception:
    pd = None

dotenv.load_dotenv()  # Load .env if exists, for AWS credentials or other config
all_services = []  # Global variable to hold services for log parsing

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

def list_objects(s3, bucket, start_date, end_date, prefix=f"new/AWSLogs/099702455984/elasticloadbalancing/eu-west-1"):
    paginator = s3.get_paginator("list_objects_v2")
    if start_date.month == end_date.month:
        prefix = f"{prefix}/{start_date.year}/{start_date.month:02d}/"
    else:
        prefix = f"{prefix}/{start_date.year}/"
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
        try:
            req_proc = float(parts[5])
        except Exception:
            req_proc = 0
        try:
            backend_proc = float(parts[6])
        except Exception:
            backend_proc = 0
        try:
            resp_proc = float(parts[7])
        except Exception:
            resp_proc = 0
        elb_status = parts[8]
        backend_status = parts[9]
        request = parts[12] if len(parts) > 12 else ""
        pattern = r'/services/([^/]+/[^/?\s]+)'
        match = re.search(pattern, request)
        if match:
            mapservice = match.group(1).replace("/", ".")
        else:
            pattern = r'https://geobank\.bymoslo\.no:443/Geocortex/Essentials/REST/viewers/geobank\.geobank'
            if re.search(pattern, request, re.IGNORECASE):
                mapservice = "Geobank"
            else:
                mapservice = ""
        return {
            "timestamp": timestamp,
            "processing_time": req_proc + resp_proc + backend_proc,
            "elb_status": elb_status,
            "backend_status": backend_status,
            "request": request,
            "service": mapservice,
        }

def analyze_files(paths):
    total = 0
    status_counter = Counter()
    url_counter = Counter()
    total_req_time = 0.0
    req_time_count = 0
    all_lines = []
    i = 1
    for p in paths:
        with open_maybe_gz(p) as fh:
            print(f'\rAnalyzing file {i}/{len(paths)}', end="", flush=True)
            for line in fh:
                parsed = parse_elb_line(line.strip())
                if not parsed:
                    continue
                all_lines.append(parsed)
                total += 1
                status = parsed.get("elb_status") or parsed.get("backend_status")
                if status is not None:
                    try:
                        sc = int(status)
                        bucket = f"{sc//100}xx"
                        status_counter[bucket] += 1
                    except Exception:
                        status_counter[str(status)] += 1
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
        i += 1

    result = {
        "total_requests": total,
        "status_counts": dict(status_counter),
        "top_urls": url_counter.most_common(20),
        "avg_processing_time": (total_req_time / req_time_count) if req_time_count else None,
        "all_lines": all_lines,
    }
    print("\nAnalysis complete.")
    return result

def find_local_log_files(directory):
    p = Path(directory)
    if not p.exists():
        return []
    files = [str(p / f) for f in sorted(os.listdir(directory)) if f.endswith(".log") or f.endswith(".gz") or f.endswith(".txt")]
    return files

def iso_to_dt(s):
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)

def export_result(analysis_result, output_path):
    """Write analysis results to Excel file with multiple sheets."""
    if pd is None:
        print("pandas is required for Excel output. Install from requirements.txt and try again.")
        return
    all_lines = analysis_result.get("all_lines", [])
    if all_lines:
        lines_df = pd.DataFrame(all_lines)  # Excel row limit
        lines_df.to_parquet(output_path.replace(".xlsx", ".parquet"), index=False)  # Save full data as Parquet for larger datasetsuv a
        #return
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
            lines_df = pd.DataFrame(all_lines[:1048575])  # Excel row limit
            lines_df.to_excel(writer, sheet_name="All Logs", index=False)
            lines_df.to_parquet(output_path.replace(".xlsx", ".parquet"), index=False)  # Save full data as Parquet for larger datasetsuv a
        
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
    parser.add_argument("--start-date", help="ISO start date (inclusive), e.g. 2026-02-01")
    parser.add_argument("--end-date", help="ISO end date (inclusive)")
    args = parser.parse_args()
    s3 = get_s3_client(profile=os.environ["AWSPROFILE"], region=os.environ["AWSREGION"])
    
    if args.start_date:
        start = iso_to_dt(args.start_date)
    else:
        yesterday = (datetime.now() - timedelta(days=1)).date()
        start = yesterday
    if args.end_date:
        end = iso_to_dt(args.end_date)
    else:
        end = yesterday + timedelta(days=1)
    print(f"Looking for log files from {start} to {end} in S3 bucket {os.environ['S3BUCKET']}...")
    objs = list(list_objects(s3, os.environ["S3BUCKET"], start, end))
    print(f"Found {len(objs)} log files in S3 bucket {os.environ['S3BUCKET']}")
    to_download = []
    for o in objs:
        lm = o.get("LastModified")
        if start and lm.replace(tzinfo=timezone.utc) < start:
            continue
        if end and lm.replace(tzinfo=timezone.utc) > end:
            continue
        to_download.append(o.get("Key"))

    print(f"Found {len(to_download)} objects to download")
    temp_dir = os.environ.get("TEMPDIR", "elb_logs")
    try:
        shutil.rmtree(temp_dir)
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"Could not remove old temp dir: {e}")
    os.makedirs(temp_dir, exist_ok=True)
    downloaded = []
    downloaded = downloaded + [os.path.join(temp_dir, f) for f in os.listdir(temp_dir)]
    for key in to_download:
        target = os.path.join(temp_dir, os.path.basename(key))
        ok = download_object(s3, os.environ["S3BUCKET"], key, target)
        if ok:
                downloaded.append(target)

    if not downloaded:
        print("No log files to analyze.")
        return
    print(f"Analyzing {len(downloaded)} files...")
    summary = analyze_files(downloaded)
    #print("Analysis summary:")
    #print(json.dumps(summary, indent=2, default=str))
    print("Writing Excel report...")
    export_result(summary, os.environ.get("EXCELFILE", "karttjenester.xlsx"))
    try:
        shutil.rmtree(temp_dir)
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"Could not remove temp dir: {e}")


if __name__ == "__main__":
    #line = 'h2 2026-02-01T19:40:27.217624Z app/alb-new-gis-prod/51fa1287639c83c0 88.89.241.253:54514 10.2.64.202:6443 0.029 0.001 0.000 200 200 57 4186 "GET https://geodata.bymoslo.no:443/arcgis/rest/services/geodata/Parkering/MapServer/3?f=json HTTP/2.0" "Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.2 Mobile/15E148 Safari/604.1" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:eu-west-1:099702455984:targetgroup/tg-geodata-linux-rest/1fc05f6eee69c7ec "Root=1-697fac2b-37c1883c146f782b4c0531b4" "geodata.bymoslo.no" "arn:aws:acm:eu-west-1:099702455984:certificate/bb069ebc-f9d2-4737-8999-8b7f6772a012" 114 2026-02-01T19:40:27.187000Z "waf,forward" "-" "-" "10.2.64.202:6443" "200" "-" "-" TID_13c6539d4d103340847ff94500482a22 "-" "-" "-"'
    #r = parse_elb_line(line)
    #print(r)
    main()
