#!/usr/bin/env python3
"""Propagate the doi.gov TEMPLATE notebook to all other domains.

`domain_analysis/url_structure_doi.ipynb` is the single source of truth.
Edit that notebook (it runs against the local doi sample DB at
data/04_doi/cdxj.duckdb), then re-run this script to regenerate every other
domain's notebook from it.

Substitutions applied per domain (in order):
    04_doi     -> NN_<name>      (DB folder)
    doi.gov    -> <domain>
    doi_con    -> <name>_con     (connection variable)
    doi        -> <name>         (leftover tokens; word-boundary regex so e.g.
                                  'doing' is never touched)

Cell outputs and execution counts are stripped in the generated copies.

Usage:
    python generate_domain_notebooks.py                  # all domains except the template
    python generate_domain_notebooks.py --domains ed.gov usda.gov
"""

import argparse
import copy
import json
import re
from pathlib import Path

from config import TARGET_DOMAINS
from split_domains import domain_folder_name

TEMPLATE_DOMAIN = "doi.gov"
TEMPLATE_PATH = Path("domain_analysis/url_structure_doi.ipynb")
OUT_DIR = Path("domain_analysis")


def short(domain: str) -> str:
    return domain.removesuffix(".gov").replace(".", "_")


def derive_notebook(template: dict, domain: str) -> dict:
    folder = domain_folder_name(domain, TARGET_DOMAINS)
    template_folder = domain_folder_name(TEMPLATE_DOMAIN, TARGET_DOMAINS)
    t_short = short(TEMPLATE_DOMAIN)
    d_short = short(domain)

    # Leftover bare-token sub: 'doi' not embedded in a longer word ('doing' safe),
    # underscores/digits allowed as neighbours ('doi_seg1', '04_doi' handled earlier).
    leftover = re.compile(rf"(?<![A-Za-z]){re.escape(t_short)}(?![A-Za-z])")

    nb = copy.deepcopy(template)
    for cell in nb["cells"]:
        src = "".join(cell["source"]) if isinstance(cell["source"], list) else cell["source"]
        src = src.replace(template_folder, folder)
        src = src.replace(TEMPLATE_DOMAIN, domain)
        src = src.replace(f"{t_short}_con", f"{d_short}_con")
        src = leftover.sub(d_short, src)
        cell["source"] = src.splitlines(keepends=True)
        if cell["cell_type"] == "code":
            cell["outputs"] = []
            cell["execution_count"] = None
    return nb


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--domains", nargs="+", default=None,
        help="Domains to generate. Default: every TARGET_DOMAIN except the template.",
    )
    args = parser.parse_args()

    domains = args.domains or [d for d in TARGET_DOMAINS if d != TEMPLATE_DOMAIN]

    template = json.loads(TEMPLATE_PATH.read_text())
    for domain in domains:
        if domain not in TARGET_DOMAINS:
            raise SystemExit(f"Unknown domain: {domain}")
        if domain == TEMPLATE_DOMAIN:
            print(f"SKIP {domain} — it is the template")
            continue
        out = OUT_DIR / f"url_structure_{short(domain)}.ipynb"
        out.write_text(json.dumps(derive_notebook(template, domain), indent=1))
        print(f"Wrote {out}")


if __name__ == "__main__":
    main()
