from pathlib import Path

bronze_path = "/app/data/raw"
trusted_path = "/app/trusted"
delivery_path = "/app/delivered"

# Data Context do Great Expectations. Volume compartilhado (`gx_docs`), servido
# por nginx em http://localhost:8182 — por isso e persistente, e nao efemero.
gx_path = "/app/gx"
