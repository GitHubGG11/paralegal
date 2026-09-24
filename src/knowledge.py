"""Indexed retrieval over normalized email/JSON records; never execute model SQL."""
import json
import re

from workers import DurableObject


def validate_records(payload):
    records = json.loads(payload)
    if not isinstance(records, list) or not 1 <= len(records) <= 100:
        raise ValueError("Send an array of 1 to 100 records per batch")
    clean = []
    for item in records:
        if not isinstance(item, dict):
            raise ValueError("Each record must be an object")
        row = {name: item.get(name, "") for name in ("id", "title", "text", "source")}
        for name, limit in (("id", 200), ("title", 500), ("text", 40000), ("source", 1000)):
            if not isinstance(row[name], str) or len(row[name]) > limit:
                raise ValueError("Invalid record " + name)
        if not row["id"].strip() or not row["text"].strip():
            raise ValueError("Each record needs a nonempty id and text")
        clean.append(row)
    return clean


def search_expression(query):
    # Literal tokens only. FTS operators, quotes and model-produced SQL are not accepted.
    terms = list(dict.fromkeys(re.findall(r"\w+", query.casefold(), re.UNICODE)))[:16]
    return " OR ".join('"' + term + '"' for term in terms)


class KnowledgeBase(DurableObject):
    def __init__(self, ctx, env):
        super().__init__(ctx, env)
        self.ctx.storage.sql.exec("""CREATE TABLE IF NOT EXISTS records (
            id TEXT PRIMARY KEY, title TEXT NOT NULL, text TEXT NOT NULL, source TEXT NOT NULL);
            CREATE VIRTUAL TABLE IF NOT EXISTS record_search USING fts5(
                id UNINDEXED, title, text, source UNINDEXED, tokenize='unicode61');""")

    async def ingest(self, payload):
        records = validate_records(payload)
        def write():
            for row in records:
                values = tuple(row[k] for k in ("id", "title", "text", "source"))
                self.ctx.storage.sql.exec("""INSERT INTO records VALUES (?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET title=excluded.title, text=excluded.text, source=excluded.source""", *values)
                rowid = self.ctx.storage.sql.exec("SELECT rowid FROM records WHERE id = ?", row["id"]).one().rowid
                self.ctx.storage.sql.exec("DELETE FROM record_search WHERE rowid = ?", rowid)
                self.ctx.storage.sql.exec("INSERT INTO record_search(rowid,id,title,text,source) VALUES (?, ?, ?, ?, ?)", rowid, *values)
        self.ctx.storage.transactionSync(write)
        return json.dumps({"imported": len(records), "total": self.count()})

    def count(self):
        return self.ctx.storage.sql.exec("SELECT count(*) AS n FROM records").one().n

    async def status(self):
        return json.dumps({"records": self.count()})

    async def search(self, query):
        if not isinstance(query, str) or len(query) > 500:
            raise ValueError("Invalid query")
        expression = search_expression(query)
        if not expression:
            return "[]"
        rows = self.ctx.storage.sql.exec("""SELECT id, title, source,
            snippet(record_search, 2, '', '', ' … ', 60) AS excerpt
            FROM record_search WHERE record_search MATCH ? ORDER BY bm25(record_search, 0, 3, 1, 0)
            LIMIT 5""", expression).toArray()
        return json.dumps([dict(id=r.id, title=r.title, source=r.source, excerpt=r.excerpt[:1800]) for r in rows])
