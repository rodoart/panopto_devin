"""Módulo checkpoint con la(s) clase(s) Checkpoint."""

import hashlib
import json
import os
from typing import Any, Callable, Dict, Optional

from pyspark.sql import DataFrame, SparkSession

from mecv.config.tables import PROCESS_CONFIG
from mecv.logging import get_logger

logger = get_logger(__name__)


class Checkpoint:
    """Persistencia temporal de DataFrames en parquet con clave determinística.

    Si el parquet ya existe y no está vacío, ``compute`` devuelve el DataFrame
    almacenado sin volver a ejecutar la función de cómputo. Esto permite
    coordinar reejecuciones en entornos con recursos limitados o en pruebas.
    """

    def __init__(
        self,
        spark: SparkSession,
        base_path: Optional[str] = None,
        prefix: str = "mecv_checkpoints",
    ) -> None:
        """Inicializa una nueva instancia de Checkpoint.

        Args:
            spark: sesión Spark activa.
            base_path: ruta base donde se escribirán los checkpoints. Si no se
                proporciona, se usa ``MECV_CHECKPOINT_BASE`` o
                ``<hdfs_staging_base>/<prefix>``.
            prefix: prefijo por defecto para la carpeta de checkpoints.
        """
        self.spark = spark
        if base_path is None:
            base_path = os.environ.get(
                "MECV_CHECKPOINT_BASE",
                f"{PROCESS_CONFIG.hdfs_staging_base}/{prefix}",
            )
        self.base_path = base_path.rstrip("/")

    @staticmethod
    def _hash(inputs: Dict[str, Any]) -> str:
        """Genera un hash determinístico a partir de un diccionario."""
        payload = json.dumps(inputs, sort_keys=True, ensure_ascii=False, default=str)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]

    def _path(self, key: str, suffix: str = "data") -> str:
        """Construye la ruta parquet para una clave."""
        return f"{self.base_path}/{key}/{suffix}.parquet"

    def exists(self, inputs: Dict[str, Any], suffix: str = "data") -> bool:
        """Devuelve True si el checkpoint existe y tiene al menos una fila."""
        path = self._path(self._hash(inputs), suffix)
        try:
            df = self.spark.read.parquet(path)
            return df.count() > 0
        except Exception:
            return False

    def read(self, inputs: Dict[str, Any], suffix: str = "data") -> DataFrame:
        """Lee el checkpoint correspondiente a ``inputs``."""
        path = self._path(self._hash(inputs), suffix)
        return self.spark.read.parquet(path)

    def write(
        self,
        df: DataFrame,
        inputs: Dict[str, Any],
        suffix: str = "data",
        mode: str = "overwrite",
    ) -> str:
        """Escribe ``df`` como checkpoint.

        Args:
            df: DataFrame a persistir.
            inputs: diccionario usado para generar la clave del checkpoint.
            suffix: sufijo que distingue checkpoints dentro de una misma clave.
            mode: modo de escritura de Spark (por defecto ``overwrite``).

        Returns:
            Ruta donde se escribió el checkpoint.
        """
        path = self._path(self._hash(inputs), suffix)
        df.write.mode(mode).option("compression", "snappy").parquet(path)
        return path

    def compute(
        self,
        inputs: Dict[str, Any],
        func: Callable[[], DataFrame],
        suffix: str = "data",
    ) -> DataFrame:
        """Devuelve el DataFrame cacheado o lo computa y persiste.

        Args:
            inputs: diccionario que conforma la clave determinística.
            func: callable sin argumentos que retorna el DataFrame a cachear.
            suffix: sufijo del checkpoint.

        Returns:
            DataFrame cacheado o recién calculado.
        """
        key = self._hash(inputs)
        path = self._path(key, suffix)
        if self.exists(inputs, suffix):
            logger.info(f"checkpoint hit: {path}")
            return self.read(inputs, suffix)
        logger.info(f"checkpoint miss: {path}")
        df = func()
        self.write(df, inputs, suffix)
        return df

    def clear(self, inputs: Dict[str, Any], suffix: str = "data") -> None:
        """Elimina el checkpoint asociado a ``inputs`` si existe."""
        key = self._hash(inputs)
        path = self._path(key, suffix)
        try:
            jvm = self.spark._jvm
            fs = jvm.org.apache.hadoop.fs.FileSystem.get(self.spark._jsc.hadoopConfiguration())
            p = jvm.org.apache.hadoop.fs.Path(path)
            if fs.exists(p):
                fs.delete(p, True)
                logger.info(f"checkpoint cleared: {path}")
        except Exception as exc:
            logger.warning(f"could not clear checkpoint {path}: {exc}")
