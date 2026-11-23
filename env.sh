# Variables de entorno para correr la práctica en modo local
# Uso: source env.sh
export JAVA_HOME="${JAVA_HOME:-$HOME/.local/jdk/jdk-17.0.21.1+2-jre}"
export PATH="$JAVA_HOME/bin:$PATH"
RAIZ_PRACTICA="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PYSPARK_PYTHON="$RAIZ_PRACTICA/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
export SPARK_LOCAL_IP=127.0.0.1   # evita que Spark busque la IP de la red
