#!/usr/bin/env bash
# Ejecuta el notebook completo (guarda las salidas en el mismo archivo)
# y genera una copia en HTML dentro de entrega/.
set -euo pipefail
cd "$(dirname "$0")"
source env.sh

.venv/bin/jupyter nbconvert --to notebook --execute --inplace \
  --ExecutePreprocessor.timeout=600 notebooks/practica_rdd.ipynb

# Quita los avisos de arranque de la JVM (stderr) para que no ensucien el notebook
.venv/bin/python - <<'PY'
import json
ruta = "notebooks/practica_rdd.ipynb"
nb = json.load(open(ruta, encoding="utf-8"))
for celda in nb["cells"]:
    if celda["cell_type"] == "code":
        celda["outputs"] = [o for o in celda["outputs"] if o.get("name") != "stderr"]
json.dump(nb, open(ruta, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
PY

.venv/bin/jupyter nbconvert --to html notebooks/practica_rdd.ipynb --output-dir entrega

echo "Listo: notebooks/practica_rdd.ipynb ejecutado y entrega/practica_rdd.html generado."
echo "Para el PDF: jupyter nbconvert --to pdf (requiere LaTeX) o imprimir el HTML a PDF."
