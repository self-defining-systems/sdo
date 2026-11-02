# Astronomy Helm dependency fixtures

These are the unmodified release chart archives declared by the pinned astronomy
parent chart (opentelemetry-demo 0.40.4, application commit
7d7b074714345a0c282b0be75af7a2c504b44c95). They retain upstream chart metadata
and license files. The resource contract renders the real charts from a cold
temporary directory, without network access, Helm repositories, or locally built
dependencies. A parent dependency version change requires replacing the matching
archive; the test checks the packaged chart identity before rendering.

Release repositories are recorded in the parent Chart.yaml:
OpenTelemetry, Jaeger, prometheus-community, Grafana, and OpenSearch.

SHA-256 checksums:

```
a875189a6871c2ed0e9695f7a03a0b939db6632a0e3a939d5ff6cd4109d8c8dd  grafana-10.5.8.tgz
91b1745059bc855e338161cd705ffae739bd965bd685a294c53d526f79fcd18d  jaeger-4.3.0.tgz
0e0462d08513d9028727f9cc4bfd452aadd8ebca26a4a0344122d0f8c1982a40  opensearch-3.4.0.tgz
fe9d90e24d4bfbe42fc9bb4ab8352edf1d560c7c6cf7b268f668ec8ce67d3afd  opentelemetry-collector-0.142.2.tgz
07e818a16ad184ffe3a3b5b99b059038ef3204b9bb5fb87c717cc8e674278f3b  prometheus-28.2.0.tgz
```
