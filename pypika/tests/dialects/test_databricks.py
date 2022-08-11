import unittest

from pypika import (
    Tables,
    functions as fn,
    Column,
)
from pypika import DatabricksQuery


class QuoteTests(unittest.TestCase):
    table_abc, table_efg = Tables("abc", "efg")

    def test_use_double_quotes_on_alias_but_not_on_terms(self):
        q = DatabricksQuery.from_(self.table_abc).select(self.table_abc.a.as_("bar"))

        self.assertEqual('SELECT a "bar" FROM abc', q.get_sql(with_namespace=True))

    def test_use_double_quotes_on_alias_but_not_on_terms_with_joins(self):
        foo = self.table_abc.as_("foo")
        bar = self.table_efg.as_("bar")

        q = DatabricksQuery.from_(foo).join(bar).on(foo.fk == bar.id).select(foo.a, bar.b)

        self.assertEqual(
            "SELECT foo.a,bar.b " 'FROM abc "foo" ' 'JOIN efg "bar" ' "ON foo.fk=bar.id",
            q.get_sql(with_namespace=True),
        )

    def test_use_double_quotes_on_alias_but_not_on_terms(self):
        idx = self.table_abc.index.as_("idx")
        val = fn.Sum(self.table_abc.value).as_("val")
        q = DatabricksQuery.from_(self.table_abc).select(idx, val).groupby(idx).orderby(idx)

        self.assertEqual(
            'SELECT index "idx",SUM(value) "val" ' "FROM abc " 'GROUP BY "idx" ' 'ORDER BY "idx"',
            q.get_sql(with_namespace=True),
        )

    def test_dont_use_double_quotes_on_joining_queries(self):
        foo = self.table_abc
        bar = self.table_efg
        q1 = DatabricksQuery.from_(foo).select(foo.b)
        q2 = DatabricksQuery.from_(bar).select(bar.b)
        q = DatabricksQuery.from_(q1).join(q2).on(q1.b == q2.b).select("*")

        self.assertEqual(
            "SELECT * " 'FROM (SELECT b FROM abc) sq0 ' 'JOIN (SELECT b FROM efg) sq1 ' "ON sq0.b=sq1.b",
            q.get_sql(),
        )

    def test_dont_use_double_quotes_on_create_queries(self):
        q = DatabricksQuery.create_table(self.table_abc).columns(Column("id", "INT"))
        self.assertEqual("CREATE TABLE abc (id INT)", q.get_sql())

    def test_dont_use_double_quotes_on_drop_queries(self):
        q = DatabricksQuery.drop_table(self.table_abc)
        self.assertEqual("DROP TABLE abc", q.get_sql())
