#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Lança um Dataflow Flex Template e (opcional) drena um job existente.
Requisitos:
  pip install google-api-python-client google-auth google-auth-httplib2 google-auth-oauthlib

Uso (exemplos):
  python3 run_dataflow.py launch \
    --project insider-lake-sensitive \
    --region southamerica-east1 \
    --job-name muninn-avro-to-parquet-v18 \
    --template "gs://insider-lake-ingestion-br/templates/avro_to_parquet.json" \
    --service-account "worker-dataflow@insider-lake-sensitive.iam.gserviceaccount.com" \
    --network "projects/insider-lake-sensitive/global/networks/muninn-conversion-network" \
    --subnetwork "projects/insider-lake-sensitive/regions/southamerica-east1/subnetworks/muninn-conversion-subnet" \
    --staging "gs://dataflow-staging-southamerica-east1-423568297638" \
    --temp "gs://dataflow-staging-southamerica-east1-423568297638/tmp" \
    --no-public-ips \
    --param input_glob="gs://insider-lake-ingestion-br/muninn_backups/avro/*.avro" \
    --param output_prefix="gs://insider-lake-ingestion-br/muninn_backups/parquet/" \
    --param poll_interval_sec=60

  python run_dataflow.py drain \
    --project insider-lake-sensitive \
    --region southamerica-east1 \
    --job-id 2025-10-15_10_44_08-1437069842677636789
"""

import argparse
import sys
from typing import Dict

import google.auth
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


def parse_kv_params(items) -> Dict[str, str]:
    params = {}
    if not items:
        return params
    for it in items:
        if "=" not in it:
            raise ValueError(f"Parâmetro inválido (use chave=valor): {it}")
        k, v = it.split("=", 1)
        params[k] = v
    return params


def build_env(args) -> Dict:
    env = {}
    if args.service_account:
        env["serviceAccountEmail"] = args.service_account
    if args.network:
        env["network"] = args.network
    if args.subnetwork:
        env["subnetwork"] = args.subnetwork
    if args.temp:
        env["tempLocation"] = args.temp
    if args.staging:
        env["stagingLocation"] = args.staging   # <-- essencial
    # IPs públicos/privados
    if args.no_public_ips:
        env["ipConfiguration"] = "WORKER_IP_PRIVATE"
    elif args.public_ips:
        env["ipConfiguration"] = "WORKER_IP_PUBLIC"
    # (opcionais úteis)
    if args.worker_region:
        env["workerRegion"] = args.worker_region
    if args.num_workers:
        env["numWorkers"] = args.num_workers
    if args.max_workers:
        env["maxWorkers"] = args.max_workers
    if args.labels:
        env["additionalUserLabels"] = parse_kv_params(args.labels)
    return env


def cmd_launch(args):
    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    df = build("dataflow", "v1b3", credentials=creds, cache_discovery=False)

    parameters = parse_kv_params(args.param)

    body = {
        "launchParameter": {
            "jobName": args.job_name,
            "containerSpecGcsPath": args.template,  # Flex Template JSON no GCS
            "parameters": parameters,
            "environment": build_env(args),
            # define como false para não bloquear esperando; true retorna estado final
            "update": False,
        },
        "validateOnly": False,
    }

    try:
        resp = df.projects().locations().flexTemplates().launch(
            projectId=args.project, location=args.region, body=body
        ).execute()
        # A resposta contém o job criado (quando sucesso)
        job = resp.get("job", {})
        job_id = job.get("id")
        state = job.get("currentState")
        print(f"[OK] Job criado: name={job.get('name')} id={job_id} state={state}")
        print(f"   region={args.region} serviceAccount={args.service_account}")
        if args.no_public_ips:
            print("   ipConfiguration=WORKER_IP_PRIVATE")
        elif args.public_ips:
            print("   ipConfiguration=WORKER_IP_PUBLIC")
        if not job_id:
            print("[Aviso] API não retornou job.id; verifique no Console do Dataflow.")
    except HttpError as e:
        print("[ERRO] Falha ao lançar o template:")
        print(e)
        sys.exit(1)


def cmd_drain(args):
    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    df = build("dataflow", "v1b3", credentials=creds, cache_discovery=False)

    body = {
        "requestedState": "JOB_STATE_DRAINING"
    }
    try:
        resp = df.projects().locations().jobs().update(
            projectId=args.project,
            location=args.region,
            jobId=args.job_id,
            body=body
        ).execute()
        print(f"[OK] Job em DRAINING: {args.job_id} (region={args.region})")
        # opcional: imprimir estado atual
        state = resp.get("currentState")
        if state:
            print(f"Estado atual reportado: {state}")
    except HttpError as e:
        print("[ERRO] Falha ao pedir drain do job:")
        print(e)
        sys.exit(1)


def main():
    p = argparse.ArgumentParser(description="Launcher para Dataflow Flex Template (launch/drain)")
    sub = p.add_subparsers(dest="cmd", required=True)

    # LAUNCH
    pl = sub.add_parser("launch", help="Lança um Flex Template")
    pl.add_argument("--project", required=True)
    pl.add_argument("--region", required=True)
    pl.add_argument("--job-name", required=True)
    pl.add_argument("--template", required=True, help="gs://.../template.json (Flex)")
    pl.add_argument("--service-account", required=True)
    pl.add_argument("--network", required=True,
                    help="projects/PROJ/global/networks/NOME")
    pl.add_argument("--subnetwork", required=True,
                    help="projects/PROJ/regions/REGION/subnetworks/NOME")
    pl.add_argument("--staging", required=True, help="gs://bucket (para UI; aqui só conferimos)")
    pl.add_argument("--temp", required=True, help="gs://bucket/tmp")
    # IPs
    ip = pl.add_mutually_exclusive_group()
    ip.add_argument("--no-public-ips", action="store_true", help="WORKER_IP_PRIVATE + requer NAT+PGA")
    ip.add_argument("--public-ips", action="store_true", help="WORKER_IP_PUBLIC")
    # Outros opcionais úteis
    pl.add_argument("--worker-region", help="Override da região de workers (normalmente igual à --region)")
    pl.add_argument("--num-workers", type=int)
    pl.add_argument("--max-workers", type=int)
    pl.add_argument("--labels", action="append", help="labels chave=valor (repita a flag)")
    pl.add_argument("--param", action="append", help="parâmetros do template chave=valor (repita a flag)")
    pl.set_defaults(func=cmd_launch)

    # DRAIN
    pd = sub.add_parser("drain", help="Coloca um job existente em DRAINING")
    pd.add_argument("--project", required=True)
    pd.add_argument("--region", required=True)
    pd.add_argument("--job-id", required=True, help="ID do job Dataflow")
    pd.set_defaults(func=cmd_drain)

    args = p.parse_args()
    # Validações rápidas
    for must_be_gcs in (args.staging,) if args.cmd == "launch" else ():
        if not must_be_gcs.startswith("gs://"):
            p.error("--staging deve começar com gs://")
    if args.cmd == "launch" and not (args.no_public_ips or args.public_ips):
        print("[Info] Nenhuma flag de IPs informada; usando padrão do serviço (costuma ser público).")

    args.func(args)


if __name__ == "__main__":
    main()
