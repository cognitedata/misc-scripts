#!/usr/bin/env python3

import os
from typing import Dict, List, Set, Tuple

from cognite.client._api.sequences import SequencesAPI
from cognite.client.data_classes import Sequence
from cognite.client.experimental import CogniteClient

class _SequencesAPI06(SequencesAPI):
    def list_all(self):
        c = None
        seq = []
        while True:
            res = self._get(url_path=self._RESOURCE_PATH, params={"limit": 1000, "cursor": c}).json()["data"]
            seq.extend(res["items"])
            c = res.get("nextCursor")
            if not c:
                break
        return self._LIST_CLASS._load(seq)

    def update_columns(self, seqid, column_ids, column_eids):
        col_updates = [{"id": id, "externalId": {"set": eid}} for id, eid in zip(column_ids, column_eids)]
        return self._post(url_path=f"/sequences/{seqid}/columns/update/", json={"items": col_updates}).json()["data"]

    def create_sequences(self, sequences: List[Sequence]):
        return self._post(url_path="/sequences", json={"items": [s.dump(camel_case=True) for s in sequences]})


def _generate_new_eids(columns: List[Dict]) -> Tuple[List[int], List[str]]:
    column_names = [c["name"] for c in columns if c.get("name") is not None]
    column_ids = []
    new_column_eids = []
    for i, c in enumerate(columns):
        c_name = c.get("name")
        c_id = c["id"]
        if c.get("externalId") is not None:
            new_column_eid = c["externalId"]
        elif c_name is not None and column_names.count(c_name) == 1:
            new_column_eid = c_name
        else:
            new_column_eid = str(c_id)
        new_column_eids.append(new_column_eid)
        column_ids.append(c_id)
    return column_ids, new_column_eids


def migrate_sequence_column_external_ids(client: CogniteClient):
    seq_list = client.sequences06.list_all()
    for seq in seq_list:
        print(f"id: {seq.id}, externalid: {seq.external_id}")
        if None in seq.column_external_ids:
            column_ids, new_column_eids = _generate_new_eids(seq.columns)
            client.sequences06.update_columns(seq.id, column_ids, new_column_eids)
            print(f"MIGRATED EXTERNAL IDS.\nOld: {seq.column_external_ids}\nNew: {new_column_eids}\n")
        else:
            print(f"NO CHANGE\nOld: {seq.column_external_ids}\nNew: {seq.column_external_ids}\n")


if __name__ == "__main__":
    key = os.environ["CDF_API_KEY"]
    cluster = os.environ["CDF_CLUSTER"]
    project = os.environ["CDF_PROJECT"]
    client = CogniteClient(
        project=project, api_key=key, base_url=cluster, client_name="sequences-id-migration-{}".format(project)
    )
    client.sequences06 = _SequencesAPI06(client._config, api_version="0.6", cognite_client=client)
    migrate_sequence_column_external_ids(client)
    
