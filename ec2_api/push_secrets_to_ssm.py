#!/usr/bin/env python3
"""
push_secrets_to_ssm.py — One-time helper to rotate and upload secrets to SSM.

Run this ONCE locally (or on a trusted machine) after rotating credentials.
Requires AWS credentials with ssm:PutParameter permission.

Usage:
    python push_secrets_to_ssm.py --region ap-northeast-1 --prefix /bess/app

The script will:
  1. Prompt for each secret value (no echo to terminal)
  2. Upload as SecureString (KMS-encrypted) to SSM Parameter Store
  3. Print the ARNs so you can verify in AWS Console

After this script runs:
  - Update your .env with the NEW values for local dev only
  - Redeploy / restart the EC2 service to pick up rotated secrets
"""

import argparse
import getpass
import sys

import boto3
from botocore.exceptions import ClientError


SECRETS = [
    {
        "name": "server_secret",
        "description": "BESS API JWT signing secret",
        "hint": "Generate with: python -c \"import secrets; print(secrets.token_hex(32))\"",
    },
    {
        "name": "db_pass",
        "description": "RDS PostgreSQL password",
        "hint": "Paste the NEW password after rotating it on RDS Console first",
    },
]


def push_secret(ssm, prefix: str, name: str, value: str, description: str):
    path = f"{prefix}/{name}"
    try:
        ssm.put_parameter(
            Name=path,
            Value=value,
            Type="SecureString",
            Description=description,
            Overwrite=True,   # Rotate: overwrite existing value
        )
        print(f"  [OK] {path} uploaded as SecureString")
    except ClientError as exc:
        print(f"  [FAIL] {path}: {exc}", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Upload BESS secrets to SSM Parameter Store")
    parser.add_argument("--region", default="ap-northeast-1")
    parser.add_argument("--prefix", default="/bess/app")
    args = parser.parse_args()

    print(f"\nTarget SSM prefix : {args.prefix}")
    print(f"Region            : {args.region}")
    print("-" * 50)
    print("IMPORTANT: Rotate credentials on AWS Console BEFORE entering new values here.\n")

    ssm = boto3.client("ssm", region_name=args.region)

    for secret in SECRETS:
        name = secret["name"]
        print(f"\n[{name}]")
        print(f"  Hint: {secret['hint']}")
        value = getpass.getpass(f"  Enter new value (hidden): ").strip()
        if not value:
            print("  Skipped (empty input).")
            continue
        confirm = getpass.getpass(f"  Confirm value           : ").strip()
        if value != confirm:
            print("  ERROR: Values do not match. Aborting.", file=sys.stderr)
            sys.exit(1)
        push_secret(ssm, args.prefix, name, value, secret["description"])

    print("\nAll secrets uploaded. Verify at:")
    print(f"  https://{args.region}.console.aws.amazon.com/systems-manager/parameters")
    print("\nNext steps:")
    print("  1. Restart EC2 service: sudo systemctl restart ec2_api")
    print(f"  2. Update your local .env: SERVER_SECRET=<new> DB_PASS=<new>")


if __name__ == "__main__":
    main()
