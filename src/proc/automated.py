import json
import csv
from typing import List, Tuple, Optional, Union, Generator
from pathlib import Path

import pandas as pd
import numpy as np
from elastipy import Search, query, connections
from tqdm import tqdm

from .proc import *

connections.set("default", {"timeout": 60})


class AutomatedCommitsProc(ProcBase):

    NAME = "automated-commits"

    @classmethod
    def add_arguments(cls, parser: ArgumentParser):
        parser.add_argument(
            "--min-doc-count", type=int, default=20,
            help="Number of minimum PushEvents per repo"
        )
        parser.add_argument(
            "--spectrum-size", type=int, default=28,
            help="Size of each auto-correlation spectrum"
        )

    def __init__(self, min_doc_count: int, spectrum_size: int):
        self.min_doc_count = min_doc_count
        self.spectrum_size = spectrum_size

    def search(self) -> Search:
        return Search("gharchive-push-2018")

    def run(self):
        doc_counts = self.get_doc_count()
        print(len(doc_counts))
        repos = sorted(doc_counts)

        self.render_timelines(
            search=self.search(),
            field="repo", terms=repos,
        )
        self.render_spectra(len(repos))

    def get_doc_count(self):
        filename = f"doc-count-min-{self.min_doc_count}.json"
        cfilename = self.cache_filename(filename)
        if cfilename.exists():
            data = json.loads(cfilename.read_text())
        else:
            data = big_doc_count(
                search=self.search(),
                field="repo",
                min_count=self.min_doc_count,
            )
            self.cache_makedirs(filename)
            cfilename.write_text(json.dumps(data, indent=2))

        return data

    def iter_timelines(self, batch_size: int = 1000) -> Generator[pd.DataFrame, None, None]:
        filename = "timelines.csv"
        cfilename = self.cache_filename(filename)

        columns = None
        with open(str(cfilename)) as fp:

            rows = []
            for row in csv.reader(fp):
                if columns is None:
                    columns = row
                else:
                    rows.append(row)

                if len(rows) >= batch_size:
                    yield (
                        pd.DataFrame(rows, columns=columns)
                        .set_index("repo").replace("", np.nan).astype(float)
                    )
                    rows = []
        if rows:
            yield (
                pd.DataFrame(rows, columns=columns)
                .set_index("repo").replace("", np.nan).astype(float)
            )

    def render_timelines(
            self,
            search: Search,
            field: str,
            terms: List[str],
            interval: str = "1d",
            batch_size: int = 200_000 // 370,
    ) -> Path:
        filename = "timelines.csv"

        cfilename = self.cache_filename(filename)
        if cfilename.exists() and cfilename.stat().st_size:
            return cfilename

        self.cache_makedirs(filename)
        with cfilename.open("w") as fp:
            writer = csv.writer(fp)
            writer_header = True

            terms = list(terms)
            for idx in tqdm(range(0, len(terms), batch_size), desc="rendering timelines"):
                df = (search
                    .terms(field, terms[idx:idx+batch_size])
                    .agg_terms(field, field=field, size=len(terms[idx:idx+batch_size]))
                    .agg_date_histogram("date", calendar_interval=interval)
                    .execute().df()
                    .set_index(["date", field])
                    ["date.doc_count"].unstack(field).T
                )

                for idx, row in df.iterrows():
                    if writer_header:
                        writer.writerow(["repo"] + list(row.index))
                        writer_header = False

                    writer.writerow([idx] + row.replace(np.nan, "").tolist())

        return cfilename

    def render_spectra(self, num_repos: int) -> Path:
        filename = f"spectra-s{self.spectrum_size}.csv"
        cfilename = self.cache_filename(filename)

        if cfilename.exists() and cfilename.stat().st_size:
            return cfilename

        self.cache_makedirs(filename)
        with cfilename.open("w") as fp:
            writer = csv.writer(fp)
            writer_header = True

            for df in tqdm(self.iter_timelines(batch_size=1000), total=num_repos // 1000, desc="rendering spectra"):
                for name, tl in df.iterrows():
                    spec = auto_correlate(tl, self.spectrum_size)

                    if writer_header:
                        writer.writerow(["repo"] + list(range(len(spec))))
                        writer_header = False

                    writer.writerow([name] + spec.tolist())

        return cfilename


def big_doc_count(
        search: Search,
        field: str,
        max_size: Optional[int] = None,
        min_count: Optional[int] = None,
        batch_size: int = 100000,
        max_terms: int = 65000,
) -> dict:
    result = dict()
    batch_search = search.copy()
    while True:
        agg = batch_search.copy().agg_terms(field, field=field, size=batch_size)
        batch_result = agg.execute().to_dict()
        result.update(batch_result)

        actual_min_count = min(result.values())

        print(f"have {len(result)} docs, min-count: {actual_min_count}, took", agg.search.response["took"], "ms")
        if len(batch_result) < batch_size:
            break
        if max_size and len(result) >= max_size:
            break
        if min_count and result and actual_min_count < min_count:
            break

        exclude_terms = list(batch_result.keys())
        while exclude_terms:
            batch_search = batch_search & ~query.Terms(field, exclude_terms[:max_terms])
            exclude_terms = exclude_terms[max_terms:]

    return result


def auto_correlate(s: Union[pd.Series, np.ndarray], size: int) -> np.ndarray:
    if isinstance(s, pd.Series):
        s = s.values
    s = np.nan_to_num(s)
    seqs = np.concatenate(
        [
            s[i:-(size-i+1)].reshape(1, -1)
            for i in range(1, size+1)
        ],
        axis=0
    )
    ac = np.corrcoef(s[:-size-1], seqs)
    return ac[0, 1:]
