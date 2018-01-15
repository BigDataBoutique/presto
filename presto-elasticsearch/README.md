# Presto Elasticsearch Connector

"elasticsearch" is a Presto Catalog using this connector. A Catalog can refer to multiple clusters.

An Elasticsearch Index and Type pairs in the format `{index_name}/{type_name}` is a Presto Table

A list of such Index-Type pairs in a signle cluster, is a Schema in that Catalog

The Mapping under Index-Type in Elasticsearch is used to figure out Columns in the Table and their data type.

TODO: support partitioning via index naming pattern