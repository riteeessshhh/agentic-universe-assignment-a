"""Workflow schema and disposition catalogue loader."""
from __future__ import annotations

import csv
import json
from dataclasses import dataclass, field
from pathlib import Path

from .normalizers import normalize_header


@dataclass(frozen=True)
class FieldSpec:
    name: str
    field_type: str
    required: bool
    pii: bool
    reference: bool
    values: list[str]
    default: str | None


@dataclass(frozen=True)
class WorkflowSchema:
    workflow_id: str
    fields: list[FieldSpec]
    reference_field: str
    phone_field: str
    alias_lookup: dict[str, str] = field(repr=False)


@dataclass(frozen=True)
class DispositionSpec:
    workflow_id: str
    code: str
    label: str
    is_terminal: bool
    is_success: bool


def load_schemas(schemas_dir: str | Path) -> tuple[dict[str, WorkflowSchema], list[DispositionSpec]]:
    """Load all workflow JSON schemas and dispositions.csv from schemas_dir."""
    root = Path(schemas_dir)
    if not root.is_dir():
        raise FileNotFoundError(f"Schemas directory not found: {root}")

    workflows: dict[str, WorkflowSchema] = {}
    for json_path in sorted(root.glob("*.json")):
        raw = json.loads(json_path.read_text(encoding="utf-8"))
        wf_id = raw["workflow_id"]
        fields: list[FieldSpec] = []
        alias_lookup: dict[str, str] = {}
        ref_field: str | None = None
        phone_field: str | None = None

        for f in raw["fields"]:
            spec = FieldSpec(
                name=f["name"],
                field_type=f["type"],
                required=bool(f.get("required", False)),
                pii=bool(f.get("pii", False)),
                reference=bool(f.get("reference", False)),
                values=list(f.get("values", [])),
                default=f.get("default"),
            )
            fields.append(spec)
            for raw_name in [f["name"], *f.get("aliases", [])]:
                alias_lookup[normalize_header(raw_name)] = spec.name
            if spec.reference:
                ref_field = spec.name
            if spec.field_type == "phone":
                phone_field = spec.name

        if ref_field is None or phone_field is None:
            raise ValueError(f"Workflow {wf_id} must define both reference and phone fields")

        workflows[wf_id] = WorkflowSchema(
            workflow_id=wf_id,
            fields=fields,
            reference_field=ref_field,
            phone_field=phone_field,
            alias_lookup=alias_lookup,
        )

    disp_path = root / "dispositions.csv"
    if not disp_path.is_file():
        raise FileNotFoundError(f"Dispositions catalogue not found: {disp_path}")

    dispositions: list[DispositionSpec] = []
    with disp_path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            dispositions.append(
                DispositionSpec(
                    workflow_id=row["workflow_id"].strip(),
                    code=row["code"].strip(),
                    label=row["label"].strip(),
                    is_terminal=row["is_terminal"].strip().lower() == "true",
                    is_success=row["is_success"].strip().lower() == "true",
                )
            )

    return workflows, dispositions
