# Presto Elasticsearch Connector

We map Elasticsearch concepts to SQL catalog/schema/table/column as follows:

```
Catalog = cluster
Schema = part of cluster (index prefix?), currently we ignore schema name completely
Table = index+type
Column = document field
```

An Elasticsearch Index and Type pairs in the format `{index_name}/{type_name}` is represented in Presto as a Table.

The Mapping under Index-Type in Elasticsearch is used to figure out Columns in the Table and their data type.

A catalog maps to a `connector + (optional) configuration`, so you can have multiple catalogs (catalog is an instance of a connector) of the same connector. 


TODO: support partitioning via index naming pattern

TODO: review https://prestodb.io/docs/current/connector/redshift.html

TODO: Suppot geo-spatial queries https://prestodb.io/docs/current/functions/geospatial.html
